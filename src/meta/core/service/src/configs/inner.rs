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
use databend_common_meta_types::node::Node;
use databend_common_tracing::Config as LogConfig;

use super::outer_v0::Config as OuterV0Config;

/// Configuration for the gRPC API server.
///
/// This struct holds settings for the gRPC endpoint that serves client requests,
/// including the listening address, optional advertise host for cluster communication,
/// and TLS certificates for secure connections.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct GrpcConfig {
    /// The address the gRPC server listens on, e.g., "0.0.0.0:9191".
    pub api_address: String,

    /// Optional hostname to advertise to other nodes in the cluster.
    /// If set, this host combined with the port from `api_address` forms the
    /// address other nodes use to connect to this server.
    pub advertise_host: Option<String>,

    /// Path to the TLS certificate file for the gRPC server.
    /// Leave empty to disable TLS.
    pub tls_server_cert: String,

    /// Path to the TLS private key file for the gRPC server.
    /// Leave empty to disable TLS.
    pub tls_server_key: String,
}

impl GrpcConfig {
    /// Returns `true` if TLS is enabled (both cert and key are provided).
    pub fn tls_enabled(&self) -> bool {
        !self.tls_server_key.is_empty() && !self.tls_server_cert.is_empty()
    }

    /// Returns the advertise address if `advertise_host` is set.
    /// The address is formed by combining `advertise_host` with the port from `api_address`.
    pub fn advertise_address(&self) -> Option<String> {
        if let Some(h) = &self.advertise_host {
            // Safe unwrap(): Config::validate() ensures api_address is valid.
            let a: SocketAddr = self.api_address.parse().unwrap();
            Some(format!("{}:{}", h, a.port()))
        } else {
            None
        }
    }
}

/// Arguments for KV API commands.
///
/// This struct holds the parameters needed to execute KV API operations
/// like upsert, get, mget, and list.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct KvApiArgs {
    /// Keys to operate on.
    pub key: Vec<String>,

    /// Value to store (for upsert operations).
    pub value: String,

    /// Optional TTL in seconds for the key.
    pub expire_after: Option<u64>,

    /// Prefix for list operations.
    pub prefix: String,
}

/// Configuration for the Admin HTTP API server.
///
/// This struct holds settings for the HTTP endpoint that serves administrative
/// requests such as health checks, metrics, and cluster management operations.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct AdminConfig {
    /// The address the admin HTTP server listens on, e.g., "0.0.0.0:28002".
    pub api_address: String,

    /// Path to the TLS certificate file for the admin server.
    /// Leave empty to disable TLS.
    pub tls_server_cert: String,

    /// Path to the TLS private key file for the admin server.
    /// Leave empty to disable TLS.
    pub tls_server_key: String,
}

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
    pub admin: AdminConfig,
    pub grpc: GrpcConfig,
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
            admin: AdminConfig {
                api_address: "127.0.0.1:28002".to_string(),
                tls_server_cert: "".to_string(),
                tls_server_key: "".to_string(),
            },
            grpc: GrpcConfig {
                api_address: "127.0.0.1:9191".to_string(),
                advertise_host: None,
                tls_server_cert: "".to_string(),
                tls_server_key: "".to_string(),
            },
            raft_config: Default::default(),
        }
    }
}

impl Config {
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`OuterV0Config`] and then convert from [`OuterV0Config`] to [`Config`].
    ///
    /// In the future, we could have `ConfigV1` and `ConfigV2`.
    pub fn load() -> Result<Self, MetaStartupError> {
        let cfg = OuterV0Config::load(true)?
            .try_into()
            .map_err(MetaStartupError::InvalidConfig)?;

        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), MetaStartupError> {
        let _a: SocketAddr = self.grpc.api_address.parse().map_err(|e| {
            MetaStartupError::InvalidConfig(format!(
                "{} while parsing {}",
                e, self.grpc.api_address
            ))
        })?;
        Ok(())
    }

    /// # NOTE
    ///
    /// This function is served for tests only.
    pub fn load_for_test() -> Result<Self, MetaStartupError> {
        let cfg: Self = OuterV0Config::load(false)?
            .try_into()
            .map_err(MetaStartupError::InvalidConfig)?;
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
        .with_grpc_advertise_address(self.grpc.advertise_address())
    }

    /// Extract KV API arguments from config
    pub fn kv_api_args(&self) -> KvApiArgs {
        KvApiArgs {
            key: self.key.clone(),
            value: self.value.clone(),
            expire_after: self.expire_after,
            prefix: self.prefix.clone(),
        }
    }
}
