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

use std::collections::HashMap;
use std::future;
use std::mem;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::http_client::GLOBAL_HICKORY_POSITIVE_MIN_TTL;
use databend_common_base::http_client::get_global_hickory_resolver;
use databend_common_base::http_client::get_global_http_client;
use databend_common_metrics::storage::metrics_inc_storage_http_requests_count;
use futures::TryStreamExt;
use http::Request;
use http::Response;
use lru::LruCache;
use opendal::Buffer;
use opendal::raw::HttpBody;
use opendal::raw::HttpFetch;
use opendal::raw::parse_content_encoding;
use opendal::raw::parse_content_length;
use url::Url;

use crate::EndpointPolicyScope;
use crate::check_storage_endpoint_url;
use crate::endpoint_policy::EndpointUrlCheck;

const PINNED_CLIENT_CACHE_SIZE: usize = 32;
// Derive the cached check result's TTL from the resolver's positive_min_ttl so
// the entry stays valid for at least as long as the DNS record it was based on.
const CHECKED_ENDPOINT_CACHE_TTL: Duration = GLOBAL_HICKORY_POSITIVE_MIN_TTL;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct EndpointCheckCacheKey {
    scheme: String,
    host: String,
    port: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PinnedClientCacheKey {
    scheme: String,
    host: String,
    port: u16,
    resolved_addrs: Vec<SocketAddr>,
}

pub struct StorageHttpClient {
    client: reqwest::Client,
    pool_max_idle_per_host: usize,
    connect_timeout: u64,
    keepalive: u64,
    endpoint_policy_scope: EndpointPolicyScope,
    // Per-endpoint pinned clients keyed by (scheme, host, port, resolved_addrs).
    //
    // We cannot reuse `client` for External-scope endpoints because reqwest
    // would perform a fresh DNS lookup for every request, reopening the TOCTOU
    // window between policy validation and the actual connection. An attacker
    // who controls DNS could pass validation with a public IP and then switch
    // the record to an internal address before reqwest connects.
    //
    // Instead, after check_storage_endpoint_url resolves and validates the IPs,
    // we build a dedicated client with resolve_to_addrs pinned to those checked
    // addresses. That client never re-resolves the host, so the connection is
    // guaranteed to reach the validated IP regardless of subsequent DNS changes.
    pinned_clients: Mutex<LruCache<PinnedClientCacheKey, reqwest::Client>>,
    // TTL cache for endpoint check results keyed by (scheme, host, port).
    // Avoids calling resolve_global_dns on every HttpFetch::fetch for the same
    // external endpoint. TTL is aligned with the hickory resolver's
    // positive_min_ttl so cached results stay valid at least as long as the
    // underlying DNS entry.
    checked_endpoints: Mutex<LruCache<EndpointCheckCacheKey, (EndpointUrlCheck, Instant)>>,
}

pub fn get_storage_http_client(
    pool_max_idle_per_host: usize,
    connect_timeout: u64,
    keepalive: u64,
    endpoint_policy_scope: EndpointPolicyScope,
) -> StorageHttpClient {
    StorageHttpClient {
        client: get_global_http_client(pool_max_idle_per_host, connect_timeout, keepalive).inner(),
        pool_max_idle_per_host,
        connect_timeout,
        keepalive,
        endpoint_policy_scope,
        pinned_clients: Mutex::new(LruCache::new(
            NonZeroUsize::new(PINNED_CLIENT_CACHE_SIZE).expect("cache size must be greater than 0"),
        )),
        checked_endpoints: Mutex::new(LruCache::new(
            NonZeroUsize::new(PINNED_CLIENT_CACHE_SIZE).expect("cache size must be greater than 0"),
        )),
    }
}

impl StorageHttpClient {
    fn client_for_checked_endpoint(
        &self,
        url: &Url,
        resolved_addrs: Option<&[SocketAddr]>,
    ) -> opendal::Result<reqwest::Client> {
        let Some(resolved_addrs) = resolved_addrs else {
            return Ok(self.client.clone());
        };

        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        let port = url.port_or_known_default().unwrap_or_default();
        let cache_key = PinnedClientCacheKey::new(url.scheme(), &host, port, resolved_addrs);

        let client = self
            .pinned_clients
            .lock()
            .unwrap()
            .try_get_or_insert(cache_key, || {
                // Use the same hickory resolver as the global client so that
                // any non-pinned hostname (e.g. a proxy from env vars) is also
                // resolved via hickory rather than the system resolver.
                // Redirects are disabled: the single validated host property
                // must be preserved — a redirect to a different host would
                // bypass the egress policy check. This applies to both
                // hostname endpoints (closing the DNS-rebinding TOCTOU
                // window) and IP-literal endpoints (closing a 30x
                // bounce-to-internal bypass), since check_url_with_dns now
                // populates resolved_addrs for IP literals as well.
                let mut builder = reqwest::ClientBuilder::new()
                    .http1_only()
                    .use_native_tls()
                    .dns_resolver(get_global_hickory_resolver())
                    .redirect(reqwest::redirect::Policy::none())
                    .pool_max_idle_per_host(self.pool_max_idle_per_host)
                    .connect_timeout(Duration::from_secs(self.connect_timeout))
                    .resolve_to_addrs(&host, resolved_addrs);

                if self.keepalive != 0 {
                    builder = builder.tcp_keepalive(Duration::from_secs(self.keepalive));
                }

                builder.build().map_err(|err| {
                    opendal::Error::new(opendal::ErrorKind::Unexpected, err.to_string())
                })
            })?
            .clone();

        Ok(client)
    }

    async fn check_endpoint_cached(&self, url: &Url) -> opendal::Result<EndpointUrlCheck> {
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        let port = url.port_or_known_default().unwrap_or_default();
        let key = EndpointCheckCacheKey {
            scheme: url.scheme().to_ascii_lowercase(),
            host,
            port,
        };

        {
            let mut cache = self.checked_endpoints.lock().unwrap();
            if let Some((check, inserted_at)) = cache.get(&key) {
                if inserted_at.elapsed() < CHECKED_ENDPOINT_CACHE_TTL {
                    return Ok(check.clone());
                }
            }
        }

        let check = check_storage_endpoint_url(url).await.map_err(|err| {
            opendal::Error::new(opendal::ErrorKind::PermissionDenied, err.to_string())
        })?;

        self.checked_endpoints
            .lock()
            .unwrap()
            .put(key, (check.clone(), Instant::now()));

        Ok(check)
    }
}

impl PinnedClientCacheKey {
    fn new(scheme: &str, host: &str, port: u16, resolved_addrs: &[SocketAddr]) -> Self {
        let mut resolved_addrs = resolved_addrs.to_vec();
        resolved_addrs.sort_unstable();
        resolved_addrs.dedup();

        Self {
            scheme: scheme.to_ascii_lowercase(),
            host: host.to_ascii_lowercase(),
            port,
            resolved_addrs,
        }
    }
}

impl HttpFetch for StorageHttpClient {
    async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;

        let url = Url::parse(uri.to_string().as_str()).expect("input request url must be valid");
        let host = url.host_str().unwrap_or_default();
        let method = match req.method() {
            &http::Method::GET => {
                let query: HashMap<_, _> = url.query_pairs().collect();
                match query.get("list-type") {
                    Some(_) => "LIST",
                    None => "GET",
                }
            }
            m => m.as_str(),
        };
        // get first component in path as bucket name
        let bucket = match url.path_segments() {
            Some(mut segments) => segments.next().unwrap_or("-"),
            None => "-",
        };
        metrics_inc_storage_http_requests_count(
            host.to_string(),
            method.to_string(),
            bucket.to_string(),
        );

        let (parts, body) = req.into_parts();

        let client = if self.endpoint_policy_scope == EndpointPolicyScope::External {
            // check_endpoint_cached resolves and validates the target IPs,
            // caching the result for CHECKED_ENDPOINT_CACHE_TTL to avoid a
            // DNS lookup on every request to the same endpoint.
            let checked_endpoint = self.check_endpoint_cached(&url).await?;
            // Pin the checked addresses into reqwest so the actual request
            // does not perform a second uncontrolled DNS lookup, and disable
            // redirects so a 30x cannot bounce to a different host. This
            // covers IP-literal endpoints too: their resolved_addrs is the
            // literal IP itself, so they take the same pinned, redirect-
            // disabled path as hostname endpoints.
            self.client_for_checked_endpoint(&url, checked_endpoint.resolved_addrs.as_deref())?
        } else {
            self.client.clone()
        };

        let mut req_builder = client
            .request(
                parts.method,
                reqwest::Url::from_str(&uri.to_string()).expect("input request url must be valid"),
            )
            .headers(parts.headers);

        req_builder = req_builder.version(parts.version);
        // Don't set body if body is empty.
        if !body.is_empty() {
            req_builder = req_builder.body(reqwest::Body::wrap_stream(body))
        }

        let mut resp = req_builder
            .send()
            .await
            .map_err(|err| to_opendal_unexpected_error(err, &uri, "send http request"))?;

        // Get content length from header so that we can check it.
        //
        // - If the request method is HEAD, we will ignore content length.
        // - If response contains content_encoding, we should omit its content length.
        let content_length = if is_head || parse_content_encoding(resp.headers())?.is_some() {
            None
        } else {
            parse_content_length(resp.headers())?
        };

        let mut hr = Response::builder()
            .status(resp.status())
            // Insert uri into response extension so that we can fetch
            // it later.
            .extension(uri.clone());

        hr = hr.version(resp.version());

        // Swap headers directly instead of copy the entire map.
        mem::swap(hr.headers_mut().unwrap(), resp.headers_mut());

        let bs = HttpBody::new(
            resp.bytes_stream()
                .try_filter(|v| future::ready(!v.is_empty()))
                .map_ok(Buffer::from)
                .map_err(move |err| {
                    to_opendal_unexpected_error(err, &uri, "read data from http response")
                }),
            content_length,
        );

        let resp = hr.body(bs).expect("response must build succeed");
        Ok(resp)
    }
}

fn to_opendal_unexpected_error(err: reqwest::Error, uri: &http::Uri, desc: &str) -> opendal::Error {
    let mut oe = opendal::Error::new(opendal::ErrorKind::Unexpected, desc)
        .with_operation("http_util::Client::send")
        .with_context("url", uri.to_string());
    if is_temporary_error(&err) {
        oe = oe.set_temporary();
    }
    oe = oe.set_source(err);
    oe
}

#[inline]
fn is_temporary_error(err: &reqwest::Error) -> bool {
    // error sending request
    err.is_request() ||
        // request or response body error
        err.is_body() ||
        // error decoding response body, for example, connection reset.
        err.is_decode()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::Instant;

    use lru::LruCache;
    use url::Url;

    use super::EndpointCheckCacheKey;
    use super::PinnedClientCacheKey;
    use super::StorageHttpClient;
    use crate::EndpointPolicyScope;
    use crate::endpoint_policy::EndpointUrlCheck;

    fn addr(value: &str) -> SocketAddr {
        value.parse().unwrap()
    }

    fn make_client(scope: EndpointPolicyScope) -> StorageHttpClient {
        use databend_common_base::http_client::get_global_http_client;
        StorageHttpClient {
            client: get_global_http_client(1, 30, 0).inner(),
            pool_max_idle_per_host: 1,
            connect_timeout: 30,
            keepalive: 0,
            endpoint_policy_scope: scope,
            pinned_clients: Mutex::new(LruCache::new(NonZeroUsize::new(4).unwrap())),
            checked_endpoints: Mutex::new(LruCache::new(NonZeroUsize::new(4).unwrap())),
        }
    }

    fn populate_checked_endpoint_cache(
        client: &StorageHttpClient,
        url: &Url,
        check: EndpointUrlCheck,
        age: Duration,
    ) {
        let key = EndpointCheckCacheKey {
            scheme: url.scheme().to_ascii_lowercase(),
            host: url.host_str().unwrap_or_default().to_ascii_lowercase(),
            port: url.port_or_known_default().unwrap_or_default(),
        };
        let inserted_at = Instant::now().checked_sub(age).unwrap_or(Instant::now());
        client
            .checked_endpoints
            .lock()
            .unwrap()
            .put(key, (check, inserted_at));
    }

    fn checked_endpoint_cache_len(client: &StorageHttpClient) -> usize {
        client.checked_endpoints.lock().unwrap().len()
    }

    #[test]
    fn test_pinned_client_cache_key_normalizes_addr_order_and_duplicates() {
        let left = PinnedClientCacheKey::new("https", "Example.COM", 443, &[
            addr("93.184.216.34:443"),
            addr("93.184.216.35:443"),
            addr("93.184.216.34:443"),
        ]);
        let right = PinnedClientCacheKey::new("HTTPS", "example.com", 443, &[
            addr("93.184.216.35:443"),
            addr("93.184.216.34:443"),
        ]);

        assert_eq!(left, right);
        assert_eq!(left.resolved_addrs.len(), 2);
    }

    #[test]
    fn test_pinned_client_cache_key_separates_endpoint_parts() {
        let base =
            PinnedClientCacheKey::new("https", "example.com", 443, &[addr("93.184.216.34:443")]);

        assert_ne!(
            base,
            PinnedClientCacheKey::new("http", "example.com", 443, &[addr("93.184.216.34:443")])
        );
        assert_ne!(
            base,
            PinnedClientCacheKey::new("https", "other.example.com", 443, &[addr(
                "93.184.216.34:443"
            )])
        );
        assert_ne!(
            base,
            PinnedClientCacheKey::new("https", "example.com", 8443, &[addr("93.184.216.34:443")])
        );
        assert_ne!(
            base,
            PinnedClientCacheKey::new("https", "example.com", 443, &[addr("93.184.216.35:443")])
        );
    }

    #[test]
    fn test_checked_endpoint_cache_hit_reuses_entry() {
        let client = make_client(EndpointPolicyScope::External);
        let url = Url::parse("https://93.184.216.34:443").unwrap();
        let check = EndpointUrlCheck {
            resolved_addrs: Some(vec![addr("93.184.216.34:443")]),
        };

        // Pre-populate with a fresh entry (age = 0).
        populate_checked_endpoint_cache(&client, &url, check, Duration::ZERO);
        assert_eq!(checked_endpoint_cache_len(&client), 1);

        // Build a pinned client from the cached result — should hit the cache,
        // not perform a DNS lookup, and succeed.
        let cached_check = client
            .checked_endpoints
            .lock()
            .unwrap()
            .get(&EndpointCheckCacheKey {
                scheme: "https".to_string(),
                host: "93.184.216.34".to_string(),
                port: 443,
            })
            .map(|(check, inserted_at)| (check.clone(), *inserted_at));

        assert!(cached_check.is_some());
        let (cached, inserted_at) = cached_check.unwrap();
        assert!(inserted_at.elapsed() < super::CHECKED_ENDPOINT_CACHE_TTL);
        assert_eq!(
            cached.resolved_addrs.as_deref(),
            Some([addr("93.184.216.34:443")].as_slice())
        );
    }

    #[test]
    fn test_checked_endpoint_cache_expired_entry_is_ignored() {
        let client = make_client(EndpointPolicyScope::External);
        let url = Url::parse("https://93.184.216.34:443").unwrap();
        let check = EndpointUrlCheck {
            resolved_addrs: Some(vec![addr("93.184.216.34:443")]),
        };

        // Pre-populate with an already-expired entry (age > TTL).
        let expired_age = super::CHECKED_ENDPOINT_CACHE_TTL + Duration::from_secs(1);
        populate_checked_endpoint_cache(&client, &url, check, expired_age);

        let cached = client
            .checked_endpoints
            .lock()
            .unwrap()
            .get(&EndpointCheckCacheKey {
                scheme: "https".to_string(),
                host: "93.184.216.34".to_string(),
                port: 443,
            })
            .map(|(_, inserted_at)| inserted_at.elapsed());

        // The entry exists but its age exceeds the TTL.
        assert!(cached.is_some_and(|age| age >= super::CHECKED_ENDPOINT_CACHE_TTL));
    }

    #[test]
    fn test_trusted_scope_bypasses_endpoint_check() {
        // A Trusted-scope client must never call check_endpoint_cached, so
        // the checked_endpoints cache must remain empty after construction.
        let client = make_client(EndpointPolicyScope::Trusted);
        assert_eq!(client.endpoint_policy_scope, EndpointPolicyScope::Trusted);
        assert_eq!(checked_endpoint_cache_len(&client), 0);
    }
}
