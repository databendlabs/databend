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

/// TLS configuration for server endpoints.
///
/// This struct holds the paths to TLS certificate and private key files
/// used to secure server connections.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct TlsConfig {
    /// Path to the TLS certificate file.
    /// Leave empty to disable TLS.
    pub cert: String,

    /// Path to the TLS private key file.
    /// Leave empty to disable TLS.
    pub key: String,
}

impl TlsConfig {
    /// Returns `true` if TLS is enabled (both cert and key are provided).
    pub fn enabled(&self) -> bool {
        !self.key.is_empty() && !self.cert.is_empty()
    }
}

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

    /// TLS configuration for the gRPC server.
    pub tls: TlsConfig,
}

impl GrpcConfig {
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

/// Configuration for the Admin HTTP API server.
///
/// This struct holds settings for the HTTP endpoint that serves administrative
/// requests such as health checks, metrics, and cluster management operations.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct AdminConfig {
    /// The address the admin HTTP server listens on, e.g., "0.0.0.0:28002".
    pub api_address: String,

    /// TLS configuration for the admin server.
    pub tls: TlsConfig,
}

/// Configuration for the meta service.
///
/// This struct contains only the configuration needed by the service library
/// to run a meta node. CLI-specific fields (cmd, config_file, log, admin)
/// are kept in the cli-config crate's `MetaConfig`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct MetaServiceConfig {
    pub grpc: GrpcConfig,
    pub raft_config: RaftConfig,
}

impl Default for MetaServiceConfig {
    fn default() -> Self {
        Self {
            grpc: GrpcConfig {
                api_address: "127.0.0.1:9191".to_string(),
                advertise_host: None,
                tls: TlsConfig::default(),
            },
            raft_config: Default::default(),
        }
    }
}

impl MetaServiceConfig {
    pub fn validate(&self) -> Result<(), MetaStartupError> {
        let _a: SocketAddr = self.grpc.api_address.parse().map_err(|e| {
            MetaStartupError::InvalidConfig(format!(
                "{} while parsing {}",
                e, self.grpc.api_address
            ))
        })?;
        Ok(())
    }

    /// Create `Node` from config
    pub fn get_node(&self) -> Node {
        Node::new(
            self.raft_config.id,
            self.raft_config.raft_api_advertise_host_endpoint(),
        )
        .with_grpc_advertise_address(self.grpc.advertise_address())
    }
}
