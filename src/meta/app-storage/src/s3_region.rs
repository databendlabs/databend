// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! S3 bucket region detection for locations without an explicit region.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use log::warn;
use parking_lot::RwLock;
use tokio::time::timeout;

const DEFAULT_DETECT_REGION_TIMEOUT_SEC: u64 = 10;
const REGION_CACHE_TTL: Duration = Duration::from_secs(3600);
const REGION_CACHE_MAX_ENTRIES: usize = 1024;

type RegionCache = LazyLock<RwLock<HashMap<(String, String), (String, Instant)>>>;

/// Cache for S3 region detection results.
///
/// Key: (endpoint, bucket) — a bucket name alone is not unique because the same
/// name can exist on different S3-compatible endpoints (AWS, MinIO, OSS, etc.).
///
/// Uses `RwLock<HashMap>` rather than a concurrent map because writes are rare
/// (only on first access to a new bucket or after TTL expiry) while reads are
/// frequent. Under this workload the uncontended read-lock path is effectively
/// free, and write-side contention is negligible.
static REGION_CACHE: RegionCache = LazyLock::new(|| RwLock::new(HashMap::new()));

/// Detects the region of `bucket` on `endpoint_url`.
///
/// Returns an empty string if the region can't be detected, leaving it to the
/// downstream fallback when the operator is built.
pub(crate) async fn detect_s3_region(endpoint_url: &str, bucket: &str) -> String {
    let endpoint = normalize_s3_endpoint(endpoint_url);
    let cache_key = (endpoint.clone(), bucket.to_string());

    if let Some(cached) = get_cached_region(&cache_key) {
        return cached;
    }

    let region = match timeout(
        Duration::from_secs(DEFAULT_DETECT_REGION_TIMEOUT_SEC),
        opendal::services::S3::detect_region(&endpoint, bucket),
    )
    .await
    {
        Ok(v) => v.unwrap_or_default(),
        Err(_) => {
            warn!(
                "detect region timeout ({}s) for endpoint={}, bucket={}, \
                 will rely on downstream fallback",
                DEFAULT_DETECT_REGION_TIMEOUT_SEC, endpoint, bucket
            );
            String::new()
        }
    };

    if !region.is_empty() {
        insert_cached_region(cache_key, region.clone());
    }
    region
}

fn normalize_s3_endpoint(endpoint_url: &str) -> String {
    let endpoint = endpoint_url.trim_end_matches('/');
    if endpoint.starts_with("http") {
        endpoint.to_string()
    } else {
        format!("https://{}", endpoint)
    }
}

fn get_cached_region(key: &(String, String)) -> Option<String> {
    let cache = REGION_CACHE.read();
    if let Some((region, ts)) = cache.get(key)
        && ts.elapsed() < REGION_CACHE_TTL
    {
        return Some(region.clone());
    }
    None
}

fn insert_cached_region(key: (String, String), region: String) {
    let mut cache = REGION_CACHE.write();
    // Evict expired entries when hitting the capacity limit to prevent
    // unbounded growth (e.g. from crafted bucket names).
    if cache.len() >= REGION_CACHE_MAX_ENTRIES {
        cache.retain(|_, (_, ts)| ts.elapsed() < REGION_CACHE_TTL);
    }
    // If still at capacity after eviction, skip caching this entry.
    if cache.len() < REGION_CACHE_MAX_ENTRIES {
        cache.insert(key, (region, Instant::now()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_s3_endpoint() {
        assert_eq!(
            normalize_s3_endpoint("https://s3.amazonaws.com/"),
            "https://s3.amazonaws.com"
        );
        assert_eq!(
            normalize_s3_endpoint("https://s3.amazonaws.com"),
            "https://s3.amazonaws.com"
        );
        assert_eq!(
            normalize_s3_endpoint("s3.amazonaws.com"),
            "https://s3.amazonaws.com"
        );
        assert_eq!(
            normalize_s3_endpoint("http://minio:9000/"),
            "http://minio:9000"
        );
    }

    #[test]
    fn test_region_cache_hit_and_expiry() {
        let key = (
            "https://s3.amazonaws.com".to_string(),
            "test-bucket-cache-hit".to_string(),
        );

        // Insert a fresh entry.
        insert_cached_region(key.clone(), "us-west-2".to_string());
        assert_eq!(get_cached_region(&key), Some("us-west-2".to_string()));

        // Insert an expired entry directly to simulate TTL expiry.
        {
            let mut cache = REGION_CACHE.write();
            let expired = Instant::now() - Duration::from_secs(7200);
            cache.insert(key.clone(), ("us-west-2".to_string(), expired));
        }
        assert_eq!(get_cached_region(&key), None);
    }
}
