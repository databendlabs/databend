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

use crate::storage_params::STORAGE_S3_DEFAULT_ENDPOINT;

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
/// `aws_region` is the value of the `AWS_REGION` env var. With the AWS global
/// default endpoint it selects the regional endpoint used for detection, see
/// [`regional_probe_region`].
///
/// Returns an empty string if the region can't be detected, leaving it to the
/// downstream fallback when the operator is built.
pub(crate) async fn detect_s3_region(
    endpoint_url: &str,
    bucket: &str,
    aws_region: Option<String>,
) -> String {
    let endpoint = normalize_s3_endpoint(endpoint_url);
    let cache_key = (endpoint.clone(), bucket.to_string());

    if let Some(cached) = get_cached_region(&cache_key) {
        return cached;
    }

    // When AWS_REGION is set, ask its regional endpoint instead of the global
    // one: S3 reports the bucket's real region from any regional endpoint.
    // This also covers hosts that reach S3 only through a regional endpoint
    // (e.g. a VPC endpoint), where the global endpoint probe would time out.
    let regional_probe = regional_probe_region(&endpoint, aws_region);
    let probe = async {
        match &regional_probe {
            Some(region) => detect_region_via_regional_endpoint(region, bucket).await,
            None => opendal::services::S3::detect_region(&endpoint, bucket).await,
        }
    };

    let region = match timeout(
        Duration::from_secs(DEFAULT_DETECT_REGION_TIMEOUT_SEC),
        probe,
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

/// Returns the region whose regional endpoint should be used to detect the
/// bucket region, instead of the AWS global endpoint.
///
/// Only applies to the AWS global default endpoint with a valid `AWS_REGION`,
/// so S3-compatible services with custom endpoints keep the original behavior.
fn regional_probe_region(endpoint: &str, aws_region: Option<String>) -> Option<String> {
    if endpoint != STORAGE_S3_DEFAULT_ENDPOINT {
        return None;
    }
    let region = aws_region?.trim().to_string();
    // Only accept region-like values, so the env var can't inject into the URL.
    let valid = !region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    valid.then_some(region)
}

/// Detects the bucket region by sending an anonymous HeadBucket to the regional
/// endpoint of `region`.
///
/// S3 returns the bucket's real region in `x-amz-bucket-region` from any
/// regional endpoint, whether the status is 200, 301 or 403.
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html>
///
/// `opendal::services::S3::detect_region` can't be reused here: it parses the
/// region out of `https://s3.{region}.amazonaws.com` without sending a request.
async fn detect_region_via_regional_endpoint(region: &str, bucket: &str) -> Option<String> {
    // Same client settings as opendal's detect_region (honors proxy env vars).
    let client = reqwest::Client::new();
    detect_region_by_head(&client, &regional_probe_url(region, bucket)).await
}

/// Path-style URL, so bucket names with dots keep a valid TLS certificate.
fn regional_probe_url(region: &str, bucket: &str) -> String {
    format!("https://s3.{region}.amazonaws.com/{bucket}")
}

/// Returns the region from `x-amz-bucket-region`, or `None` if it is missing.
///
/// The status code is ignored on purpose. S3 always sets the header, so a
/// response without it did not come from S3 directly (e.g. a proxy) and the
/// bucket region is unknown. Guessing the probed region here would cache it and
/// persist it into stage meta; leaving it empty keeps the downstream
/// `AWS_REGION` fallback for this statement without storing a guess.
async fn detect_region_by_head(client: &reqwest::Client, url: &str) -> Option<String> {
    let res = client
        .head(url)
        .send()
        .await
        .map_err(|err| warn!("detect region via {url} failed: {err:?}"))
        .ok()?;

    let region = res
        .headers()
        .get("x-amz-bucket-region")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());
    if region.is_none() {
        warn!(
            "detect region via {url} got status {} without x-amz-bucket-region",
            res.status()
        );
    }
    region
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

    #[test]
    fn regional_probe_region_applies_to_default_endpoint() {
        let env = || Some("ap-northeast-1".to_string());

        // Default endpoint, with or without trailing slash / scheme.
        for endpoint in [
            "https://s3.amazonaws.com",
            "https://s3.amazonaws.com/",
            "s3.amazonaws.com",
        ] {
            assert_eq!(
                Some("ap-northeast-1".to_string()),
                regional_probe_region(&normalize_s3_endpoint(endpoint), env()),
                "endpoint: {endpoint}"
            );
        }

        // Surrounding whitespace is ignored.
        assert_eq!(
            Some("ap-northeast-1".to_string()),
            regional_probe_region(
                STORAGE_S3_DEFAULT_ENDPOINT,
                Some(" ap-northeast-1 ".to_string())
            )
        );
    }

    #[test]
    fn regional_probe_region_keeps_original_detection_otherwise() {
        // Unset, empty, or non region-like AWS_REGION.
        for aws_region in [
            None,
            Some("  ".to_string()),
            Some("evil.com/x".to_string()),
            Some("AP-NORTHEAST-1".to_string()),
        ] {
            assert_eq!(
                None,
                regional_probe_region(STORAGE_S3_DEFAULT_ENDPOINT, aws_region.clone()),
                "AWS_REGION: {aws_region:?}"
            );
        }

        // Custom endpoints (S3-compatible services, plain http) are not affected.
        for endpoint in [
            "http://s3.amazonaws.com",
            "https://s3.us-west-2.amazonaws.com",
            "http://127.0.0.1:9000",
            "https://oss-cn-hangzhou.aliyuncs.com",
            "https://account.r2.cloudflarestorage.com",
        ] {
            assert_eq!(
                None,
                regional_probe_region(
                    &normalize_s3_endpoint(endpoint),
                    Some("ap-northeast-1".to_string())
                ),
                "endpoint: {endpoint}"
            );
        }
    }

    /// Probes a local server that answers one canned HTTP `response`.
    async fn probe(response: &str) -> Option<String> {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/bucket", listener.local_addr().unwrap());
        let server = async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        };
        let client = local_client();
        let ((), region) = tokio::join!(server, detect_region_by_head(&client, &url));
        region
    }

    #[test]
    fn regional_probe_url_is_path_style() {
        assert_eq!(
            "https://s3.ap-northeast-1.amazonaws.com/my-bucket",
            regional_probe_url("ap-northeast-1", "my-bucket")
        );
        assert_eq!(
            "https://s3.us-east-1.amazonaws.com/my.dotted.bucket",
            regional_probe_url("us-east-1", "my.dotted.bucket")
        );
    }

    /// Client for local mock servers, unaffected by proxy env vars on the host.
    fn local_client() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    #[tokio::test]
    async fn detect_region_by_head_reads_bucket_region_header() {
        // Bucket in another region: S3 answers 301 with the real region.
        assert_eq!(
            Some("us-east-1".to_string()),
            probe("HTTP/1.1 301 Moved Permanently\r\nx-amz-bucket-region: us-east-1\r\ncontent-length: 0\r\n\r\n").await
        );

        // Anonymous request to the right region: 403 with the header.
        assert_eq!(
            Some("ap-northeast-1".to_string()),
            probe("HTTP/1.1 403 Forbidden\r\nx-amz-bucket-region: ap-northeast-1\r\ncontent-length: 0\r\n\r\n").await
        );

        // Authorized request: 200 with the header.
        assert_eq!(
            Some("eu-west-1".to_string()),
            probe("HTTP/1.1 200 OK\r\nx-amz-bucket-region: eu-west-1\r\ncontent-length: 0\r\n\r\n")
                .await
        );

        // Other statuses also carry the header (e.g. 400 for a malformed request).
        assert_eq!(
            Some("us-west-2".to_string()),
            probe("HTTP/1.1 400 Bad Request\r\nx-amz-bucket-region: us-west-2\r\ncontent-length: 0\r\n\r\n").await
        );
    }

    #[tokio::test]
    async fn detect_region_by_head_without_header() {
        // Without the header the region is unknown for every status, including
        // 200/403: the probed region must not be returned as a guess.
        for status in [
            "200 OK",
            "301 Moved Permanently",
            "307 Temporary Redirect",
            "400 Bad Request",
            "403 Forbidden",
            "404 Not Found",
            "500 Internal Server Error",
            "503 Service Unavailable",
        ] {
            let response = format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\n\r\n");
            assert_eq!(None, probe(&response).await, "status: {status}");
        }
    }

    #[tokio::test]
    async fn detect_region_by_head_ignores_invalid_header() {
        // Empty value, on the statuses where a guess used to be returned.
        for status in ["200 OK", "403 Forbidden"] {
            let response =
                format!("HTTP/1.1 {status}\r\nx-amz-bucket-region: \r\ncontent-length: 0\r\n\r\n");
            assert_eq!(None, probe(&response).await, "status: {status}");
        }

        // Non visible-ASCII value can't be read as a region.
        assert_eq!(
            None,
            probe("HTTP/1.1 403 Forbidden\r\nx-amz-bucket-region: r\u{e9}gion\r\ncontent-length: 0\r\n\r\n").await
        );
    }

    #[tokio::test]
    async fn detect_region_by_head_connection_refused() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        assert_eq!(
            None,
            detect_region_by_head(&local_client(), &format!("http://{addr}/bucket")).await
        );
    }

    #[tokio::test]
    async fn detect_s3_region_prefers_cache_over_regional_probe() {
        // A cached region is returned as is, even if AWS_REGION selects a
        // regional probe. If the probe ran instead, the result would come from
        // the network (or a timeout), not from the cache.
        let bucket = "test-bucket-cache-before-regional-probe";
        insert_cached_region(
            (STORAGE_S3_DEFAULT_ENDPOINT.to_string(), bucket.to_string()),
            "us-west-2".to_string(),
        );
        assert_eq!(
            "us-west-2",
            detect_s3_region(
                "https://s3.amazonaws.com/",
                bucket,
                Some("ap-northeast-1".to_string())
            )
            .await
        );
    }

    /// Region detection against a real AWS S3 bucket.
    ///
    /// The probe is an anonymous HeadBucket, the same request as in production,
    /// so these tests only need network access to AWS, not credentials. Only the
    /// detected region is asserted, not the status code, so a change of bucket
    /// permissions (403 vs 200) does not break them.
    mod real_s3 {
        use super::*;

        /// Returns `(bucket, bucket_region)` of the configured test bucket.
        fn test_bucket() -> (String, String) {
            let bucket = std::env::var("DATABEND_TEST_S3_BUCKET")
                .expect("DATABEND_TEST_S3_BUCKET must be set");
            let region = std::env::var("DATABEND_TEST_S3_REGION")
                .expect("DATABEND_TEST_S3_REGION must be set");
            (bucket, region)
        }

        /// A region other than `region`, to probe from the wrong region.
        fn other_region(region: &str) -> &'static str {
            if region == "ap-northeast-1" {
                "eu-west-1"
            } else {
                "ap-northeast-1"
            }
        }

        #[tokio::test]
        #[ignore = "requires an explicitly configured real AWS S3 bucket"]
        async fn detect_region_via_regional_endpoint_same_region() {
            let (bucket, region) = test_bucket();
            assert_eq!(
                Some(region.clone()),
                detect_region_via_regional_endpoint(&region, &bucket).await
            );
        }

        #[tokio::test]
        #[ignore = "requires an explicitly configured real AWS S3 bucket"]
        async fn detect_region_via_regional_endpoint_cross_region() {
            // Probing from another region still returns the bucket's real
            // region, so AWS_REGION is never taken as the bucket region.
            let (bucket, region) = test_bucket();
            assert_eq!(
                Some(region.clone()),
                detect_region_via_regional_endpoint(other_region(&region), &bucket).await
            );
        }

        #[tokio::test]
        #[ignore = "requires an explicitly configured real AWS S3 bucket"]
        async fn detect_region_via_regional_endpoint_missing_bucket() {
            // S3 answers a missing bucket without x-amz-bucket-region, so the
            // region must stay unknown instead of falling back to the probed one.
            let (bucket, region) = test_bucket();
            // Make sure S3 is reachable, so `None` below isn't a network error.
            assert_eq!(
                Some(region.clone()),
                detect_region_via_regional_endpoint(&region, &bucket).await
            );

            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let missing = format!("databend-test-no-such-bucket-{nanos}");
            assert_eq!(
                None,
                detect_region_via_regional_endpoint(&region, &missing).await
            );
        }

        #[tokio::test]
        #[ignore = "requires an explicitly configured real AWS S3 bucket"]
        async fn detect_s3_region_caches_regional_probe_result() {
            // Default endpoint with AWS_REGION of another region: the regional
            // probe returns the bucket's real region, which is then cached.
            let (bucket, region) = test_bucket();
            assert_eq!(
                region,
                detect_s3_region(
                    STORAGE_S3_DEFAULT_ENDPOINT,
                    &bucket,
                    Some(other_region(&region).to_string())
                )
                .await
            );
            let key = (STORAGE_S3_DEFAULT_ENDPOINT.to_string(), bucket);
            assert_eq!(Some(region), get_cached_region(&key));
        }
    }
}
