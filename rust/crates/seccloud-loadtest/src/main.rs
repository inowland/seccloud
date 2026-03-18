use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use flate2::Compression;
use flate2::write::GzEncoder;
use tracing_subscriber::EnvFilter;

/// Generate a synthetic batch of raw events for a given source.
fn generate_batch(source: &str, batch_size: usize) -> serde_json::Value {
    let departments = ["engineering", "security", "product", "data"];
    let roles = ["engineer", "analyst", "admin", "viewer"];
    let sensitivities = ["low", "medium", "high"];

    let records: Vec<serde_json::Value> = (0..batch_size)
        .map(|i| {
            let event_type = match source {
                "okta" => "login",
                "github" => "view",
                "gworkspace" => "view",
                "snowflake" => "query",
                _ => "unknown",
            };
            let resource_kind = match source {
                "okta" => "app",
                "github" => "repo",
                "gworkspace" => "document",
                "snowflake" => "dataset",
                _ => "unknown",
            };
            serde_json::json!({
                "source_event_id": format!("load-{}-{}", uuid::Uuid::now_v7(), i),
                "observed_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "actor_email": format!("user{}@example.com", i % 50),
                "actor_name": format!("User {}", i % 50),
                "department": departments[i % 4],
                "role": roles[i % 4],
                "event_type": event_type,
                "resource_id": format!("{source}:resource-{}", i % 20),
                "resource_name": format!("resource-{}", i % 20),
                "resource_kind": resource_kind,
                "sensitivity": sensitivities[i % 3],
            })
        })
        .collect();

    serde_json::json!({
        "source": source,
        "records": records,
    })
}

fn gzip_json(value: &serde_json::Value) -> Vec<u8> {
    let json = serde_json::to_vec(value).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&json).unwrap();
    encoder.finish().unwrap()
}

struct Stats {
    success: AtomicU64,
    errors: AtomicU64,
    rate_limited: AtomicU64,
    latencies_us: tokio::sync::Mutex<Vec<u64>>,
}

impl Stats {
    fn new() -> Self {
        Self {
            success: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            latencies_us: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * sorted.len() as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let gateway_url =
        std::env::var("SECCLOUD_GATEWAY_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let tokens_json = std::env::var("SECCLOUD_LOADTEST_TOKENS").unwrap_or_else(|_| {
        r#"{"okta":"dev-token","github":"dev-token-github","gworkspace":"dev-token-gws","snowflake":"dev-token-sf"}"#.into()
    });
    let source_tokens: std::collections::HashMap<String, String> =
        serde_json::from_str(&tokens_json)?;
    let target_eps: u64 = std::env::var("SECCLOUD_LOADTEST_EPS")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    let duration_secs: u64 = std::env::var("SECCLOUD_LOADTEST_DURATION")
        .unwrap_or_else(|_| "30".into())
        .parse()?;
    let batch_size: usize = std::env::var("SECCLOUD_LOADTEST_BATCH_SIZE")
        .unwrap_or_else(|_| "100".into())
        .parse()?;
    let concurrency: usize = std::env::var("SECCLOUD_LOADTEST_CONCURRENCY")
        .unwrap_or_else(|_| "16".into())
        .parse()?;

    let batches_per_sec = (target_eps / batch_size as u64).max(1);
    let total_batches = batches_per_sec * duration_secs;
    let sources = ["okta", "github", "gworkspace", "snowflake"];

    println!("=== seccloud load test ===");
    println!("  gateway:     {gateway_url}");
    println!("  target:      {target_eps} eps");
    println!("  batch size:  {batch_size}");
    println!("  duration:    {duration_secs}s");
    println!("  concurrency: {concurrency}");
    println!("  batches/sec: {batches_per_sec}");
    println!(
        "  total:       {total_batches} batches ({} events)",
        total_batches * batch_size as u64
    );
    println!();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let stats = Arc::new(Stats::new());
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

    let start = Instant::now();
    let interval = Duration::from_secs_f64(1.0 / batches_per_sec as f64);
    let mut handles = Vec::new();

    for i in 0..total_batches {
        // Rate limit: sleep to maintain target rate
        let expected_at = interval * i as u32;
        let elapsed = start.elapsed();
        if expected_at > elapsed {
            tokio::time::sleep(expected_at - elapsed).await;
        }

        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let stats = stats.clone();
        let url = format!("{gateway_url}/intake/v1/raw-events");
        let source = sources[i as usize % sources.len()];
        let token = source_tokens
            .get(source)
            .cloned()
            .unwrap_or_else(|| "dev-token".into());

        let body = gzip_json(&generate_batch(source, batch_size));

        handles.push(tokio::spawn(async move {
            let req_start = Instant::now();
            let result = client
                .post(&url)
                .header("authorization", format!("Bearer {token}"))
                .header("content-encoding", "gzip")
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await;

            let latency_us = req_start.elapsed().as_micros() as u64;

            match result {
                Ok(resp) if resp.status().as_u16() == 202 => {
                    stats.success.fetch_add(1, Ordering::Relaxed);
                    stats.latencies_us.lock().await.push(latency_us);
                }
                Ok(resp) if resp.status().as_u16() == 429 => {
                    stats.rate_limited.fetch_add(1, Ordering::Relaxed);
                }
                Ok(resp) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    tracing::warn!(%status, %body, "unexpected response");
                }
                Err(e) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %e, "request failed");
                }
            }

            drop(permit);
        }));
    }

    // Wait for all in-flight requests
    for h in handles {
        h.await?;
    }

    let total_time = start.elapsed();
    let success = stats.success.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    let rate_limited = stats.rate_limited.load(Ordering::Relaxed);
    let total = success + errors + rate_limited;
    let actual_eps = (success * batch_size as u64) as f64 / total_time.as_secs_f64();

    let mut latencies = stats.latencies_us.lock().await;
    latencies.sort();

    println!("=== results ===");
    println!("  duration:    {:.1}s", total_time.as_secs_f64());
    println!(
        "  total:       {total} batches ({} events sent)",
        total * batch_size as u64
    );
    println!(
        "  success:     {success} ({:.1}%)",
        success as f64 / total as f64 * 100.0
    );
    println!("  errors:      {errors}");
    println!("  429s:        {rate_limited}");
    println!("  actual eps:  {actual_eps:.0}");
    println!(
        "  latency p50: {:.1}ms",
        percentile(&latencies, 50.0) as f64 / 1000.0
    );
    println!(
        "  latency p95: {:.1}ms",
        percentile(&latencies, 95.0) as f64 / 1000.0
    );
    println!(
        "  latency p99: {:.1}ms",
        percentile(&latencies, 99.0) as f64 / 1000.0
    );

    let error_rate = (errors + rate_limited) as f64 / total as f64 * 100.0;
    let p99_ms = percentile(&latencies, 99.0) as f64 / 1000.0;

    println!();
    if actual_eps >= 10_000.0 && error_rate < 1.0 && p99_ms < 500.0 {
        println!("  PASS: 10K eps target met");
    } else if actual_eps >= 1_000.0 && error_rate < 5.0 {
        println!("  BASELINE: 1K+ eps with <5% errors");
    } else {
        println!(
            "  NEEDS TUNING: {actual_eps:.0} eps, {error_rate:.1}% error rate, p99={p99_ms:.1}ms"
        );
    }

    Ok(())
}
