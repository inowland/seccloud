use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Type-erased container for injecting external resources (cloud clients, etc.)
/// into the pipeline without the pipeline crate depending on them.
#[derive(Default)]
pub struct ResourceBag {
    resources: HashMap<&'static str, Box<dyn Any + Send + Sync>>,
}

impl ResourceBag {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a named resource. Overwrites any existing resource with the same key.
    pub fn insert<T: Any + Send + Sync>(&mut self, key: &'static str, value: T) {
        self.resources.insert(key, Box::new(value));
    }

    /// Retrieve a resource by key and downcast to `T`.
    pub fn get<T: Any + Send + Sync>(&self, key: &'static str) -> Option<&T> {
        self.resources.get(key)?.downcast_ref()
    }
}

impl std::fmt::Debug for ResourceBag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceBag")
            .field("keys", &self.resources.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Per-pipeline execution context shared across all transforms in a chain.
#[derive(Debug, Clone)]
pub struct Context {
    pub tenant_id: String,
    pub config: HashMap<String, String>,
    pub resources: Arc<ResourceBag>,
}

impl Context {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            config: HashMap::new(),
            resources: Arc::new(ResourceBag::new()),
        }
    }

    /// Get a config value by key.
    pub fn config_value(&self, key: &str) -> Option<&str> {
        self.config.get(key).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_bag_insert_get() {
        let mut bag = ResourceBag::new();
        bag.insert("counter", 42u64);
        bag.insert("name", String::from("hello"));

        assert_eq!(bag.get::<u64>("counter"), Some(&42u64));
        assert_eq!(bag.get::<String>("name"), Some(&String::from("hello")));
        assert!(bag.get::<u64>("missing").is_none());
    }

    #[test]
    fn resource_bag_type_mismatch_returns_none() {
        let mut bag = ResourceBag::new();
        bag.insert("val", 42u64);

        // Wrong type → None
        assert!(bag.get::<String>("val").is_none());
    }

    #[test]
    fn context_basics() {
        let mut ctx = Context::new("tenant-1");
        ctx.config.insert("max_batch".into(), "1000".into());

        assert_eq!(ctx.tenant_id, "tenant-1");
        assert_eq!(ctx.config_value("max_batch"), Some("1000"));
        assert_eq!(ctx.config_value("missing"), None);
    }

    #[test]
    fn context_with_resources() {
        let mut bag = ResourceBag::new();
        bag.insert("client", String::from("mock-s3"));

        let ctx = Context {
            tenant_id: "t".into(),
            config: HashMap::new(),
            resources: Arc::new(bag),
        };

        assert_eq!(
            ctx.resources.get::<String>("client"),
            Some(&String::from("mock-s3"))
        );
    }
}
