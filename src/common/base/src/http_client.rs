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

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::Duration;

use hickory_resolver::config::LookupIpStrategy;
use reqwest::dns::Name;
use reqwest::dns::Resolve;
use reqwest_hickory_resolver::HickoryResolver;
use reqwest_hickory_resolver::ResolverOpts;

/// Positive DNS TTL (seconds) used by the global hickory resolver.
///
/// Exposed so that downstream caches (for example the storage HTTP client's
/// checked-endpoint cache) can derive their TTL from this single source of
/// truth instead of duplicating the value.
pub const GLOBAL_HICKORY_POSITIVE_MIN_TTL: Duration = Duration::from_secs(300);

/// Global shared hickory resolver.
static GLOBAL_HICKORY_RESOLVER: LazyLock<Arc<HickoryResolver>> = LazyLock::new(|| {
    let mut opts = ResolverOpts::default();
    // Only query for the ipv4 address.
    opts.ip_strategy = LookupIpStrategy::Ipv4Only;
    // Use larger cache size for better performance.
    opts.cache_size = 1024;
    opts.positive_min_ttl = Some(GLOBAL_HICKORY_POSITIVE_MIN_TTL);
    // Negative TTL is set to 1 minute.
    opts.negative_min_ttl = Some(Duration::from_secs(60));

    Arc::new(
        HickoryResolver::default()
            // Always shuffle the DNS results for better performance.
            .with_shuffle(true)
            .with_options(opts),
    )
});

/// Global shared HTTP client for OpenDAL storage transports.
///
/// Please create your own HTTP client if you want a dedicated connection pool.
pub static GLOBAL_HTTP_CLIENT: OnceLock<HttpClient> = OnceLock::new();

/// Create an HTTP client builder that preserves storage response bytes.
///
/// Cargo features are unified across the dependency graph, so another crate can
/// enable reqwest's content decoders for this client. OpenDAL must receive the
/// encoded object bytes to keep range lengths and checksums valid.
pub fn storage_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
}

pub fn get_global_http_client(
    pool_max_idle_per_host: usize,
    connect_timeout: u64,
    keepalive: u64,
) -> &'static HttpClient {
    GLOBAL_HTTP_CLIENT.get_or_init(move || {
        let mut builder = storage_http_client_builder();

        // Disable http2 for better performance.
        builder = builder.http1_only();

        // Enforce to use native tls backend.
        builder = builder.use_native_tls();

        // Set dns resolver.
        builder = builder.dns_resolver(GLOBAL_HICKORY_RESOLVER.clone());
        // Pool max idle per host controls connection pool size.
        builder = builder.pool_max_idle_per_host(pool_max_idle_per_host);
        // Set connect timeout if need
        builder = builder.connect_timeout(Duration::from_secs(connect_timeout));
        // Enable TCP keepalive if set.
        if keepalive != 0 {
            builder = builder.tcp_keepalive(Duration::from_secs(keepalive));
        }

        let client = builder.build().expect("http client must be created");
        HttpClient { client }
    })
}

pub fn get_global_hickory_resolver() -> Arc<HickoryResolver> {
    GLOBAL_HICKORY_RESOLVER.clone()
}

pub async fn resolve_global_dns(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    let name = host
        .parse::<Name>()
        .map_err(|err| format!("invalid dns name: {err}"))?;
    let addrs = GLOBAL_HICKORY_RESOLVER
        .resolve(name)
        .await
        .map_err(|err| err.to_string())?;

    Ok(addrs.map(|addr| SocketAddr::new(addr.ip(), port)).collect())
}

/// HttpClient that used by databend.
pub struct HttpClient {
    client: reqwest::Client,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    /// Create a general-purpose HTTP client.
    ///
    /// This client may transparently decode responses when reqwest content-decoder
    /// features are enabled. Do not use it as an OpenDAL object-storage transport;
    /// use [`storage_http_client_builder`] when the encoded response bytes must be
    /// preserved.
    pub fn new() -> Self {
        let mut builder = reqwest::ClientBuilder::new();

        // Disable http2 for better performance.
        builder = builder.http1_only();

        // Enforce to use native tls backend.
        builder = builder.use_native_tls();

        // Set dns resolver.
        builder = builder.dns_resolver(GLOBAL_HICKORY_RESOLVER.clone());

        // Pool max idle per host controls connection pool size.
        // Default to no limit, set to `0` for disable it.
        let pool_max_idle_per_host = env::var("_DATABEND_INTERNAL_POOL_MAX_IDLE_PER_HOST")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(usize::MAX);
        builder = builder.pool_max_idle_per_host(pool_max_idle_per_host);

        // Connect timeout default to 30s.
        let connect_timeout = env::var("_DATABEND_INTERNAL_CONNECT_TIMEOUT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        builder = builder.connect_timeout(Duration::from_secs(connect_timeout));

        // Enable TCP keepalive if set.
        if let Ok(v) = env::var("_DATABEND_INTERNAL_TCP_KEEPALIVE")
            && let Ok(v) = v.parse::<u64>()
        {
            builder = builder.tcp_keepalive(Duration::from_secs(v));
        }

        let client = builder.build().expect("http client must be created");
        HttpClient { client }
    }

    /// Get the inner reqwest client.
    pub fn inner(&self) -> reqwest::Client {
        self.client.clone()
    }
}

#[cfg(test)]
mod tests {
    use reqwest::header::CONTENT_ENCODING;
    use reqwest::header::CONTENT_LENGTH;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::storage_http_client_builder;

    const GZIP_BODY: &[u8] = &[
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x2b, 0x4a, 0x2c, 0x57, 0x28,
        0x2e, 0xc9, 0x2f, 0x4a, 0x4c, 0x4f, 0x55, 0xc8, 0x4f, 0xca, 0x4a, 0x4d, 0x2e, 0x51, 0x48,
        0xaa, 0x2c, 0x49, 0x2d, 0xe6, 0x02, 0x00, 0xae, 0xc5, 0xf2, 0x24, 0x19, 0x00, 0x00, 0x00,
    ];

    #[tokio::test]
    async fn test_storage_http_client_preserves_encoded_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = crate::runtime::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let bytes_read = stream.read(&mut request).await.unwrap();
            assert!(bytes_read > 0);

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                GZIP_BODY.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(GZIP_BODY).await.unwrap();
        });

        let response = storage_http_client_builder()
            .build()
            .unwrap()
            .get("http://".to_string() + &address.to_string() + "/object.json.gz")
            .send()
            .await
            .unwrap();

        assert_eq!(response.headers()[CONTENT_ENCODING], "gzip");
        assert_eq!(
            response.headers()[CONTENT_LENGTH]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            GZIP_BODY.len()
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), GZIP_BODY);
        server.await.unwrap();
    }
}
