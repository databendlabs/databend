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
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::Duration;

use hickory_resolver::config::LookupIpStrategy;
use reqwest_hickory_resolver::HickoryResolver;
use reqwest_hickory_resolver::ResolverOpts;

/// Global shared hickory resolver.
static GLOBAL_HICKORY_RESOLVER: LazyLock<Arc<HickoryResolver>> = LazyLock::new(|| {
    let mut opts = ResolverOpts::default();
    // Only query for the ipv4 address.
    opts.ip_strategy = LookupIpStrategy::Ipv4Only;
    // Use larger cache size for better performance.
    opts.cache_size = 1024;
    // Positive TTL is set to 5 minutes.
    opts.positive_min_ttl = Some(Duration::from_secs(300));
    // Negative TTL is set to 1 minute.
    opts.negative_min_ttl = Some(Duration::from_secs(60));

    Arc::new(
        HickoryResolver::default()
            // Always shuffle the DNS results for better performance.
            .with_shuffle(true)
            .with_options(opts),
    )
});

/// Global shared http client.
///
/// Please create your own http client if you want dedicated http connection pool.
pub static GLOBAL_HTTP_CLIENT: OnceLock<HttpClient> = OnceLock::new();

pub fn get_global_http_client(
    pool_max_idle_per_host: usize,
    connect_timeout: u64,
    keepalive: u64,
) -> &'static HttpClient {
    GLOBAL_HTTP_CLIENT.get_or_init(move || {
        let mut builder = reqwest::ClientBuilder::new();

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
    /// Create a new http client.
    ///
    /// # Notes
    ///
    /// This client is optimized for interact with storage services.
    /// Please tune the settings if you want to use it for other purposes.
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
