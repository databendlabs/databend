// Copyright 2020 Datafuse Labs.
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

use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::configs::Config;

// Meta env.
pub const META_ADDRESS: &str = "META_ADDRESS";
pub const META_USERNAME: &str = "META_USERNAME";
pub const META_PASSWORD: &str = "META_PASSWORD";
pub const META_EMBEDDED_DIR: &str = "META_EMBEDDED_DIR";
pub const META_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "META_RPC_TLS_SERVER_ROOT_CA_CERT";
pub const META_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "META_RPC_TLS_SERVICE_DOMAIN_NAME";

/// Meta config group.
/// serde(default) make the toml de to default working.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[structopt(long, env = META_EMBEDDED_DIR, default_value = "./_meta_embedded")]
    #[serde(default)]
    pub meta_embedded_dir: String,

    #[structopt(long, env = META_ADDRESS, default_value = "", help = "MetaStore backend address")]
    #[serde(default)]
    pub meta_address: String,

    #[structopt(long, env = META_USERNAME, default_value = "", help = "MetaStore backend user name")]
    #[serde(default)]
    pub meta_username: String,

    #[structopt(long, env = META_PASSWORD, default_value = "", help = "MetaStore backend user password")]
    #[serde(default)]
    pub meta_password: String,

    #[structopt(
        long,
        default_value = "10",
        help = "Timeout for each client request, in seconds"
    )]
    #[serde(default)]
    pub meta_client_timeout_in_second: u64,

    #[structopt(
        long,
        env = "META_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value = "",
        help = "Certificate for client to identify meta rpc server"
    )]
    #[serde(default)]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "META_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    #[serde(default)]
    pub rpc_tls_meta_service_domain_name: String,
}

impl MetaConfig {
    pub fn default() -> Self {
        MetaConfig {
            meta_embedded_dir: "./_meta_embedded".to_string(),
            meta_address: "".to_string(),
            meta_username: "root".to_string(),
            meta_password: "".to_string(),
            meta_client_timeout_in_second: 10,
            rpc_tls_meta_server_root_ca_cert: "".to_string(),
            rpc_tls_meta_service_domain_name: "localhost".to_string(),
        }
    }

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
