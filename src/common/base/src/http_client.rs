// Copyright 2024 Datafuse Labs.
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
use std::time::Duration;

use reqwest_hickory_resolver::HickoryResolver;

/// Global shared hickory resolver.
static GLOBAL_HICKORY_RESOLVER: LazyLock<Arc<HickoryResolver>> =
    LazyLock::new(|| Arc::new(HickoryResolver::default()));

/// Global shared http client.
///
/// Please create your own http client if you want dedicated http connection pool.
pub static GLOBAL_HTTP_CLIENT: LazyLock<HttpClient> = LazyLock::new(HttpClient::new);

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
        if let Ok(v) = env::var("_DATABEND_INTERNAL_TCP_KEEPALIVE") {
            if let Ok(v) = v.parse::<u64>() {
                builder = builder.tcp_keepalive(Duration::from_secs(v));
            }
        }

        let client = builder.build().expect("http client must be created");
        HttpClient { client }
    }

    /// Get the inner reqwest client.
    pub fn inner(&self) -> reqwest::Client {
        self.client.clone()
    }
}
