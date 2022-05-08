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
use common_base::base::mask_string;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;
use common_meta_grpc::MetaGrpcClientConf;
use serde::Deserialize;
use serde::Serialize;

/// Meta config group.
/// TODO(xuanwo): All meta_xxx should be rename to xxx.
#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[clap(long = "meta-embedded-dir", default_value = "./_meta_embedded")]
    #[serde(alias = "meta_embedded_dir")]
    pub embedded_dir: String,

    /// MetaStore backend address
    #[clap(long = "meta-address", default_value_t)]
    #[serde(alias = "meta_address")]
    pub address: String,

    /// MetaStore backend user name
    #[clap(long = "meta-username", default_value = "root")]
    #[serde(alias = "meta_username")]
    pub username: String,

    /// MetaStore backend user password
    #[clap(long = "meta-password", default_value_t)]
    #[serde(alias = "meta_password")]
    pub password: String,

    /// Timeout for each client request, in seconds
    #[clap(long = "meta-client-timeout-in-second", default_value = "10")]
    #[serde(alias = "meta_client_timeout_in_second")]
    pub client_timeout_in_second: u64,

    /// Certificate for client to identify meta rpc serve
    #[clap(long = "meta-rpc-tls-meta-server-root-ca-cert", default_value_t)]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[clap(long = "meta-rpc-tls-meta-service-domain-name", default_value_t)]
    pub rpc_tls_meta_service_domain_name: String,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            embedded_dir: "./_meta_embedded".to_string(),
            address: "".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            client_timeout_in_second: 10,
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
            address: self.address.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            tls_conf: self.to_grpc_tls_config(),
        };

        MetaGrpcClientConf {
            meta_service_config: meta_config.clone(),
            kv_service_config: meta_config,
            client_timeout_in_second: self.client_timeout_in_second,
        }
    }
}

impl fmt::Debug for MetaConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MetaConfig")
            .field("address", &self.address)
            .field("username", &self.username)
            .field("password", &mask_string(&self.password, 3))
            .field("embedded_dir", &self.embedded_dir)
            .field("client_timeout_in_second", &self.client_timeout_in_second)
            .field(
                "rpc_tls_meta_server_root_ca_cert",
                &self.rpc_tls_meta_server_root_ca_cert,
            )
            .field(
                "rpc_tls_meta_service_domain_name",
                &self.rpc_tls_meta_service_domain_name,
            )
            .finish()
    }
}
