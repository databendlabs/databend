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

use databend_common_exception::Result;
use databend_common_meta_app::storage::STORAGE_S3_DEFAULT_ENDPOINT;
use databend_common_meta_app::storage::StorageParams;
use log::warn;
use parking_lot::RwLock;
use tokio::time::timeout;

use crate::EndpointPolicyScope;
use crate::EndpointUrlPolicyRegistry;
use crate::endpoint_policy::CompiledPolicy;
use crate::endpoint_policy::normalize_endpoint_url;

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

/// auto_detect is used to do auto detect for some storage params under async context.
///
/// `scope` tells where `sp` comes from. For `External` params (user SQL), an
/// endpoint contacted instead of the configured one (the `AWS_REGION` regional
/// endpoint for S3 region detection) must pass the endpoint egress policy.
/// The configured endpoint itself is checked by the caller beforehand, see
/// `check_storage_params_endpoints`.
///
/// - This action should be taken before storage params been passed out.
pub async fn auto_detect_storage_params(
    sp: StorageParams,
    scope: EndpointPolicyScope,
) -> Result<StorageParams> {
    let sp = match sp {
        StorageParams::S3(mut s3) if s3.region.is_empty() => {
            let policy = match scope {
                EndpointPolicyScope::Trusted => None,
                EndpointPolicyScope::External => Some(EndpointUrlPolicyRegistry::policy()),
            };
            s3.region = detect_s3_region(
                &s3.endpoint_url,
                &s3.bucket,
                std::env::var("AWS_REGION").ok(),
                policy.as_deref(),
            )
            .await;
            StorageParams::S3(s3)
        }
        v => v,
    };

    Ok(sp)
}

/// Detects the region of `bucket` on `endpoint_url`.
///
/// `aws_region` is the value of the `AWS_REGION` env var. With the AWS global
/// default endpoint it selects the regional endpoint used for detection, see
/// [`regional_endpoint`]. `policy` is the egress policy that endpoint must pass,
/// `None` for trusted server config.
///
/// Returns an empty string if the region can't be detected, leaving it to the
/// downstream fallback when the operator is built.
async fn detect_s3_region(
    endpoint_url: &str,
    bucket: &str,
    aws_region: Option<String>,
    policy: Option<&CompiledPolicy>,
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
    //
    // Choosing the endpoint (whose policy check may resolve DNS), the probe,
    // and the fallback to the configured endpoint share one deadline.
    let probe = async {
        if let Some(regional) = regional_endpoint(&endpoint, aws_region.as_deref()) {
            match check_regional_endpoint(&regional, policy).await {
                Ok(()) => return detect_region_via_regional_endpoint(&regional, bucket).await,
                Err(err) => warn!(
                    "S3 region detection skips regional endpoint {regional}, \
                     probing {endpoint} instead: {err}"
                ),
            }
        }
        opendal::services::S3::detect_region(&endpoint, bucket).await
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

/// Returns the regional endpoint of `aws_region` (e.g.
/// `https://s3.ap-northeast-1.amazonaws.com` for `ap-northeast-1`), to which
/// region detection sends its HeadBucket instead of `endpoint`.
///
/// `endpoint` is the `endpoint_url` of the S3 storage params, normalized by
/// [`normalize_s3_endpoint`]: set in `[storage.s3]` of the server config or in
/// `CONNECTION` of a SQL location, and `https://s3.amazonaws.com` if not set.
/// `aws_region` is the value of the `AWS_REGION` env var. A HeadBucket to the
/// regional endpoint returns the same bucket region as one to
/// `https://s3.amazonaws.com`, see [`detect_region_via_regional_endpoint`].
///
/// Returns `None`, so that region detection keeps sending the HeadBucket to
/// `endpoint`, unless both conditions hold:
///
/// - `endpoint` is `https://s3.amazonaws.com`, i.e. no endpoint is set (or it
///   is set to this default). Any other endpoint was set by the user:
///   S3-compatible services are not on AWS, and for regional AWS endpoints the
///   region is already known from the endpoint itself.
/// - `aws_region` is in the AWS commercial partition, the partition of
///   `https://s3.amazonaws.com`, see [`is_aws_commercial_region`]. A HeadBucket
///   only returns the region of buckets in the partition it is sent to.
fn regional_endpoint(endpoint: &str, aws_region: Option<&str>) -> Option<String> {
    if endpoint != STORAGE_S3_DEFAULT_ENDPOINT {
        return None;
    }
    let region = aws_region.filter(|region| is_aws_commercial_region(region))?;
    Some(format!("https://s3.{region}.amazonaws.com"))
}

/// Whether `region` is in the AWS commercial partition (`aws`), the partition
/// of the global endpoint `s3.amazonaws.com`.
///
/// HeadBucket only reports the bucket region within a partition, and other
/// partitions use other domains (e.g. `amazonaws.com.cn`), so probing from
/// `us-gov-*` or `cn-*` can't find a bucket the global endpoint would find.
///
/// Mirrors the `aws` partition `regionRegex` of the AWS SDKs,
/// `^(us|eu|ap|sa|ca|me|af|il|mx)\-\w+\-\d+$`, restricted to lowercase. Only
/// such values are put into the probe URL.
fn is_aws_commercial_region(region: &str) -> bool {
    const PREFIXES: [&str; 9] = ["us", "eu", "ap", "sa", "ca", "me", "af", "il", "mx"];

    let parts: Vec<&str> = region.split('-').collect();
    matches!(parts.as_slice(), [prefix, name, number]
        if PREFIXES.contains(prefix)
            && !name.is_empty()
            && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            && !number.is_empty()
            && number.bytes().all(|b| b.is_ascii_digit()))
}

/// Checks the `regional` endpoint like the configured one: a definition-time
/// check against the egress `policy`. `None` is trusted server config.
async fn check_regional_endpoint(regional: &str, policy: Option<&CompiledPolicy>) -> Result<()> {
    match policy {
        Some(policy) => {
            let url = normalize_endpoint_url(regional)?;
            policy.check_url_at_definition(&url).await
        }
        None => Ok(()),
    }
}

/// Detects the bucket region by sending an anonymous HeadBucket to the
/// `regional` endpoint.
///
/// S3 returns the bucket's real region in `x-amz-bucket-region` from any
/// regional endpoint, whether the status is 200, 301 or 403.
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html>
///
/// `opendal::services::S3::detect_region` can't be reused here: it parses the
/// region out of `https://s3.{region}.amazonaws.com` without sending a request.
async fn detect_region_via_regional_endpoint(regional: &str, bucket: &str) -> Option<String> {
    let client = probe_client_builder()
        .build()
        .map_err(|err| warn!("build region detection client failed: {err:?}"))
        .ok()?;
    // Path-style URL, so bucket names with dots keep a valid TLS certificate.
    detect_region_by_head(&client, &format!("{regional}/{bucket}")).await
}

/// Client settings for the regional probe.
///
/// Redirects are disabled: only the response headers are needed, and following
/// a redirect would contact a host the egress policy has not checked.
fn probe_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
}

/// Returns the region from `x-amz-bucket-region`, or `None` if it is missing.
///
/// The status code is ignored: S3 sets the header for an existing bucket
/// whatever the status (200 / 301 / 403), and without it (e.g. a 404 for a
/// bucket that doesn't exist) the bucket region is unknown.
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
    use crate::EndpointUrlPolicyConfig;

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
    fn regional_endpoint_only_for_default_endpoint() {
        let aws_region = Some("ap-northeast-1");

        // Default endpoint, with or without trailing slash / scheme.
        for endpoint in [
            "https://s3.amazonaws.com",
            "https://s3.amazonaws.com/",
            "s3.amazonaws.com",
        ] {
            assert_eq!(
                Some("https://s3.ap-northeast-1.amazonaws.com".to_string()),
                regional_endpoint(&normalize_s3_endpoint(endpoint), aws_region),
                "endpoint: {endpoint}"
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
                regional_endpoint(&normalize_s3_endpoint(endpoint), aws_region),
                "endpoint: {endpoint}"
            );
        }

        // AWS_REGION unset.
        assert_eq!(None, regional_endpoint(STORAGE_S3_DEFAULT_ENDPOINT, None));
    }

    #[test]
    fn regional_endpoint_only_for_aws_commercial_partition() {
        let regional = |region: &str| regional_endpoint(STORAGE_S3_DEFAULT_ENDPOINT, Some(region));

        for region in [
            "us-east-1",
            "us-west-2",
            "eu-central-1",
            "ap-northeast-1",
            "ap-southeast-5",
            "sa-east-1",
            "ca-west-1",
            "me-central-1",
            "af-south-1",
            "il-central-1",
            "mx-central-1",
        ] {
            assert_eq!(
                Some(format!("https://s3.{region}.amazonaws.com")),
                regional(region),
                "region: {region}"
            );
        }

        // Other partitions can't discover commercial buckets, keep the
        // original detection.
        for region in [
            "us-gov-west-1",
            "us-gov-east-1",
            "cn-north-1",
            "cn-northwest-1",
            "us-iso-east-1",
            "us-isob-east-1",
            "us-isof-south-1",
            "eu-isoe-west-1",
            "eusc-de-east-1",
        ] {
            assert_eq!(None, regional(region), "region: {region}");
        }

        // Not region names.
        for region in [
            "",
            " us-east-1",
            "US-EAST-1",
            "us-east",
            "us-east-",
            "us--1",
            "us-east-1a",
            "us-east-1-",
            "xx-east-1",
            "us-east_1",
            "us-east-1.evil.com",
            "evil.com/x",
        ] {
            assert_eq!(None, regional(region), "region: {region:?}");
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

    #[tokio::test]
    async fn check_regional_endpoint_applies_egress_policy() {
        let regional = "https://s3.ap-northeast-1.amazonaws.com";
        let check = |config: EndpointUrlPolicyConfig| async move {
            let policy = CompiledPolicy::try_from(&config).unwrap();
            check_regional_endpoint(regional, Some(&policy)).await
        };

        // Trusted (no policy) or allowed by the policy.
        assert!(check_regional_endpoint(regional, None).await.is_ok());
        assert!(check(EndpointUrlPolicyConfig::default()).await.is_ok());

        // Denied: detection probes the configured endpoint instead.
        assert!(
            check(EndpointUrlPolicyConfig {
                blocked_hosts: vec!["s3.ap-northeast-1.amazonaws.com".to_string()],
                ..Default::default()
            })
            .await
            .is_err()
        );
    }

    /// Client for local mock servers, unaffected by proxy env vars on the host.
    fn local_client() -> reqwest::Client {
        probe_client_builder().no_proxy().build().unwrap()
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
    }

    #[tokio::test]
    async fn detect_region_by_head_without_header() {
        // The region is unknown without the header, whatever the status.
        // 200/403 are checked because a region could be guessed from them, as
        // opendal's detect_region does for the global endpoint.
        for status in ["200 OK", "403 Forbidden"] {
            let response = format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\n\r\n");
            assert_eq!(None, probe(&response).await, "status: {status}");
        }

        // An empty value is treated as missing.
        assert_eq!(
            None,
            probe("HTTP/1.1 403 Forbidden\r\nx-amz-bucket-region: \r\ncontent-length: 0\r\n\r\n")
                .await
        );
    }

    #[tokio::test]
    async fn detect_region_by_head_does_not_follow_redirects() {
        // The header is read from the redirect response itself; its Location
        // (a closed port here) is never contacted.
        let closed = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let response = format!(
            "HTTP/1.1 301 Moved Permanently\r\nlocation: http://{closed}/bucket\r\nx-amz-bucket-region: us-east-1\r\ncontent-length: 0\r\n\r\n"
        );
        assert_eq!(Some("us-east-1".to_string()), probe(&response).await);
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
                Some("ap-northeast-1".to_string()),
                None
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

        /// Probes `bucket` from the regional endpoint of `region`.
        async fn probe_from(region: &str, bucket: &str) -> Option<String> {
            let regional = regional_endpoint(STORAGE_S3_DEFAULT_ENDPOINT, Some(region)).unwrap();
            detect_region_via_regional_endpoint(&regional, bucket).await
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
            assert_eq!(Some(region.clone()), probe_from(&region, &bucket).await);
        }

        #[tokio::test]
        #[ignore = "requires an explicitly configured real AWS S3 bucket"]
        async fn detect_region_via_regional_endpoint_cross_region() {
            // Probing from another region still returns the bucket's real
            // region, so AWS_REGION is never taken as the bucket region.
            let (bucket, region) = test_bucket();
            assert_eq!(
                Some(region.clone()),
                probe_from(other_region(&region), &bucket).await
            );
        }

        #[tokio::test]
        #[ignore = "requires an explicitly configured real AWS S3 bucket"]
        async fn detect_region_via_regional_endpoint_missing_bucket() {
            // S3 answers a missing bucket without x-amz-bucket-region, so the
            // region must stay unknown instead of falling back to the probed one.
            let (bucket, region) = test_bucket();
            // Make sure S3 is reachable, so `None` below isn't a network error.
            assert_eq!(Some(region.clone()), probe_from(&region, &bucket).await);

            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let missing = format!("databend-test-no-such-bucket-{nanos}");
            assert_eq!(None, probe_from(&region, &missing).await);
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
                    Some(other_region(&region).to_string()),
                    None
                )
                .await
            );
            let key = (STORAGE_S3_DEFAULT_ENDPOINT.to_string(), bucket);
            assert_eq!(Some(region), get_cached_region(&key));
        }
    }
}
