// Copyright 2021 Datafuse Labs.
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

use std::fmt;

use clap::Args;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;
use common_meta_grpc::MetaGrpcClientConf;
use serde::Deserialize;
use serde::Serialize;

/// Meta config group.
#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[clap(long, default_value = "./_meta_embedded")]
    #[serde(rename = "embedded_dir")]
    pub meta_embedded_dir: String,

    /// MetaStore backend address
    #[clap(long, default_value_t)]
    #[serde(rename = "address")]
    pub meta_address: String,

    /// MetaStore backend user name
    #[clap(long, default_value = "root")]
    #[serde(rename = "username")]
    pub meta_username: String,

    /// MetaStore backend user password
    #[clap(long, default_value_t)]
    #[serde(rename = "password")]
    pub meta_password: String,

    /// Timeout for each client request, in seconds
    #[clap(long, default_value = "10")]
    #[serde(rename = "client_timeout_in_second")]
    pub meta_client_timeout_in_second: u64,

    /// Certificate for client to identify meta rpc serve
    #[clap(long = "meta-rpc-tls-meta-server-root-ca-cert", default_value_t)]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[clap(long = "meta-rpc-tls-meta-service-domain-name", default_value_t)]
    pub rpc_tls_meta_service_domain_name: String,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            meta_embedded_dir: "./_meta_embedded".to_string(),
            meta_address: "".to_string(),
            meta_username: "root".to_string(),
            meta_password: "".to_string(),
            meta_client_timeout_in_second: 10,
            rpc_tls_meta_server_root_ca_cert: "".to_string(),
            rpc_tls_meta_service_domain_name: "localhost".to_string(),
        }
    }
}

impl MetaConfig {
    pub fn is_tls_enabled(&self) -> bool {
        !self.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn to_grpc_tls_config(&self) -> Option<RpcClientTlsConfig> {
        if !self.is_tls_enabled() {
            return None;
        }

        Some(RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_meta_server_root_ca_cert.clone(),
            domain_name: self.rpc_tls_meta_service_domain_name.clone(),
        })
    }

    pub fn to_grpc_client_config(&self) -> MetaGrpcClientConf {
        let meta_config = RpcClientConf {
            address: self.meta_address.clone(),
            username: self.meta_username.clone(),
            password: self.meta_password.clone(),
            tls_conf: self.to_grpc_tls_config(),
        };

        MetaGrpcClientConf {
            meta_service_config: meta_config.clone(),
            kv_service_config: meta_config,
            client_timeout_in_second: self.meta_client_timeout_in_second,
        }
    }
}

impl fmt::Debug for MetaConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "meta_address: \"{}\", ", self.meta_address)?;
        write!(f, "meta_user: \"{}\", ", self.meta_username)?;
        write!(f, "meta_password: \"******\"")?;
        write!(f, "}}")
    }
}
