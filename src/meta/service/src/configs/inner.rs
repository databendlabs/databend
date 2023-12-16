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

use std::net::SocketAddr;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::Node;
use databend_common_tracing::Config as LogConfig;

use super::outer_v0::Config as OuterV0Config;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Config {
    pub cmd: String,
    pub key: Vec<String>,
    pub value: String,
    pub expire_after: Option<u64>,
    pub prefix: String,
    pub username: String,
    pub password: String,
    pub config_file: String,
    pub log: LogConfig,
    pub admin_api_address: String,
    pub admin_tls_server_cert: String,
    pub admin_tls_server_key: String,
    pub grpc_api_address: String,
    pub grpc_api_advertise_host: Option<String>,
    /// Certificate for server to identify itself
    pub grpc_tls_server_cert: String,
    pub grpc_tls_server_key: String,
    pub raft_config: RaftConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cmd: "".to_string(),
            key: vec![],
            value: "".to_string(),
            expire_after: None,
            prefix: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            config_file: "".to_string(),
            log: LogConfig::default(),
            admin_api_address: "127.0.0.1:28002".to_string(),
            admin_tls_server_cert: "".to_string(),
            admin_tls_server_key: "".to_string(),
            grpc_api_address: "127.0.0.1:9191".to_string(),
            grpc_api_advertise_host: None,
            grpc_tls_server_cert: "".to_string(),
            grpc_tls_server_key: "".to_string(),
            raft_config: Default::default(),
        }
    }
}

impl Config {
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`OuterV0Config`] and then convert from [`OuterV0Config`] to [`Config`].
    ///
    /// In the future, we could have `ConfigV1` and `ConfigV2`.
    pub fn load() -> Result<Self, MetaStartupError> {
        let cfg = OuterV0Config::load(true)?.into();

        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), MetaStartupError> {
        let _a: SocketAddr = self.grpc_api_address.parse().map_err(|e| {
            MetaStartupError::InvalidConfig(format!(
                "{} while parsing {}",
                e, self.grpc_api_address
            ))
        })?;
        Ok(())
    }

    /// # NOTE
    ///
    /// This function is served for tests only.
    pub fn load_for_test() -> Result<Self, MetaStartupError> {
        let cfg: Self = OuterV0Config::load(false)?.into();
        Ok(cfg)
    }

    /// Transform config into the outer style.
    ///
    /// This function should only be used for end-users.
    ///
    /// For examples:
    ///
    /// - system config table
    /// - HTTP Handler
    /// - tests
    pub fn into_outer(self) -> OuterV0Config {
        OuterV0Config::from(self)
    }

    /// Create `Node` from config
    pub fn get_node(&self) -> Node {
        Node::new(
            self.raft_config.id,
            self.raft_config.raft_api_advertise_host_endpoint(),
        )
        .with_grpc_advertise_address(self.grpc_api_advertise_address())
    }

    pub fn grpc_api_advertise_address(&self) -> Option<String> {
        if let Some(h) = &self.grpc_api_advertise_host {
            // Safe unwrap(): Self::validate() ensures it is valid.
            let a: SocketAddr = self.grpc_api_address.parse().unwrap();

            let addr = format!("{}:{}", h, a.port());
            Some(addr)
        } else {
            None
        }
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.grpc_tls_server_key.is_empty() && !self.grpc_tls_server_cert.is_empty()
    }
}
