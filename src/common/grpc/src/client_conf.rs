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

use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct RpcClientTlsConfig {
    pub rpc_tls_server_root_ca_cert: String,
    pub domain_name: String,
}

impl RpcClientTlsConfig {
    pub fn enabled(&self) -> bool {
        !self.rpc_tls_server_root_ca_cert.is_empty() && !self.domain_name.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct RpcClientConf {
    pub embedded_dir: Option<String>,
    pub endpoints: Vec<String>,
    pub username: String,
    pub password: String,
    pub tls_conf: Option<RpcClientTlsConfig>,

    /// Timeout for an RPC
    pub timeout: Option<Duration>,
    /// AutoSyncInterval is the interval to update endpoints with its latest members.
    /// None disables auto-sync.
    pub auto_sync_interval: Option<Duration>,
    pub unhealthy_endpoint_evict_time: Duration,
}

impl RpcClientConf {
    /// Whether a remote metasrv is specified.
    ///
    /// - `endpoints` accepts multiple endpoint candidates.
    ///
    /// If endpoints is configured(non-empty), use remote metasrv.
    /// Otherwise, use a local embedded meta
    pub fn local_mode(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Returns a list of endpoints.
    pub fn get_endpoints(&self) -> Vec<String> {
        self.endpoints.clone()
    }
}
