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

use crate::configs::Config;

// Meta env.
pub const META_ADDRESS: &str = "META_ADDRESS";
pub const META_USERNAME: &str = "META_USERNAME";
pub const META_PASSWORD: &str = "META_PASSWORD";
pub const META_EMBEDDED_DIR: &str = "META_EMBEDDED_DIR";
pub const META_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "META_RPC_TLS_SERVER_ROOT_CA_CERT";
pub const META_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "META_RPC_TLS_SERVICE_DOMAIN_NAME";

/// Meta config group.
#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[clap(long, env = META_EMBEDDED_DIR, default_value = "./_meta_embedded")]
    pub meta_embedded_dir: String,

    #[clap(long, env = META_ADDRESS, default_value = "", help = "MetaStore backend address")]
    pub meta_address: String,

    #[clap(long, env = META_USERNAME, default_value = "", help = "MetaStore backend user name")]
    pub meta_username: String,

    #[clap(long, env = META_PASSWORD, default_value = "", help = "MetaStore backend user password")]
    pub meta_password: String,

    #[clap(
        long,
        default_value = "10",
        help = "Timeout for each client request, in seconds"
    )]
    pub meta_client_timeout_in_second: u64,

    #[clap(
        long,
        env = "META_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value = "",
        help = "Certificate for client to identify meta rpc server"
    )]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[clap(
        long,
        env = "META_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
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
    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, meta, meta_address, String, META_ADDRESS);
        env_helper!(mut_config, meta, meta_username, String, META_USERNAME);
        env_helper!(mut_config, meta, meta_password, String, META_PASSWORD);
        env_helper!(
            mut_config,
            meta,
            rpc_tls_meta_server_root_ca_cert,
            String,
            META_RPC_TLS_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config,
            meta,
            rpc_tls_meta_service_domain_name,
            String,
            META_RPC_TLS_SERVICE_DOMAIN_NAME
        );
    }

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
