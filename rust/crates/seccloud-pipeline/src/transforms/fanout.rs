use async_trait::async_trait;

use crate::context::Context;
use crate::envelope::Envelope;
use crate::error::TransformError;
use crate::transform::Transform;

type RoutePredicate = Box<dyn Fn(&Envelope, &Context) -> bool + Send + Sync>;

/// A route in a [`FanOut`] transform. Pairs a predicate with a transform.
pub struct Route {
    predicate: RoutePredicate,
    transform: Box<dyn Transform>,
}

/// Conditional routing — sends each envelope to the first matching route.
///
/// If no route matches, the envelope is passed through unchanged (default route).
pub struct FanOut {
    routes: Vec<Route>,
}

impl FanOut {
    pub fn builder() -> FanOutBuilder {
        FanOutBuilder { routes: Vec::new() }
    }
}

pub struct FanOutBuilder {
    routes: Vec<Route>,
}

impl FanOutBuilder {
    /// Add a route: envelopes matching the predicate will be processed by the transform.
    pub fn route(
        mut self,
        predicate: impl Fn(&Envelope, &Context) -> bool + Send + Sync + 'static,
        transform: impl Transform + 'static,
    ) -> Self {
        self.routes.push(Route {
            predicate: Box::new(predicate),
            transform: Box::new(transform),
        });
        self
    }

    pub fn build(self) -> FanOut {
        FanOut {
            routes: self.routes,
        }
    }
}

#[async_trait]
impl Transform for FanOut {
    fn name(&self) -> &str {
        "FanOut"
    }

    async fn process(
        &mut self,
        envelope: Envelope,
        ctx: &Context,
    ) -> Result<Vec<Envelope>, TransformError> {
        for route in &mut self.routes {
            if (route.predicate)(&envelope, ctx) {
                return route.transform.process(envelope, ctx).await;
            }
        }
        // No route matched — pass through
        Ok(vec![envelope])
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::testing::*;
    use crate::transforms::map::Map;
    use crate::transforms::passthrough::PassThrough;

    #[tokio::test]
    async fn routes_to_matching_transform() {
        let mut fanout = FanOut::builder()
            .route(
                |env: &Envelope, _| env.metadata.source == "okta",
                Map::new(|mut env: Envelope, _ctx: &Context| {
                    env.payload = Bytes::from("okta-processed");
                    Ok(env)
                }),
            )
            .route(
                |_env: &Envelope, _| true, // catch-all
                PassThrough,
            )
            .build();

        let ctx = test_context();

        // Okta envelope → Map
        let meta = crate::envelope::EventMetadata::new("b", "okta", "t");
        let okta_env = Envelope::new(Bytes::from_static(b"data"), meta);
        let result = fanout.process(okta_env, &ctx).await.unwrap();
        assert_eq!(result[0].payload, Bytes::from("okta-processed"));

        // Non-okta → PassThrough (catch-all)
        let result = fanout.process(test_envelope(b"other"), &ctx).await.unwrap();
        assert_eq!(result[0].payload, Bytes::from_static(b"other"));
    }

    #[tokio::test]
    async fn no_route_matches_passes_through() {
        let mut fanout = FanOut::builder()
            .route(|_env: &Envelope, _| false, PassThrough)
            .build();

        let ctx = test_context();
        let result = fanout.process(test_envelope(b"data"), &ctx).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].payload, Bytes::from_static(b"data"));
    }
}
