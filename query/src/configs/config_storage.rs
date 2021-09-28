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

const DEFAULT_STORAGE_TYPE: &str = "DEFAULT_STORAGE_TYPE";

// DFS Storage env.
const DFS_STORAGE_ADDRESS: &str = "DFS_STORAGE_ADDRESS";
const DFS_STORAGE_USERNAME: &str = "DFS_STORAGE_USERNAME";
const DFS_STORAGE_PASSWORD: &str = "DFS_STORAGE_PASSWORD";
const DFS_STORAGE_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "DFS_STORAGE_RPC_TLS_SERVER_ROOT_CA_CERT";
const DFS_STORAGE_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "DFS_STORAGE_RPC_TLS_SERVICE_DOMAIN_NAME";

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum StorageType {
    Dfs,
    Disk,
    S3,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct DfsStorageConfig {
    #[structopt(long, env = DFS_STORAGE_ADDRESS, default_value = "", help = "DFS storage backend address")]
    #[serde(default)]
    pub address: String,

    #[structopt(long, env = DFS_STORAGE_USERNAME, default_value = "", help = "DFS storage backend user name")]
    #[serde(default)]
    pub username: String,

    #[structopt(long, env = DFS_STORAGE_PASSWORD, default_value = "", help = "DFS storage backend user password")]
    #[serde(default)]
    pub password: String,

    #[structopt(
        long,
        env = "DFS_STORAGE_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value = "",
        help = "Certificate for client to identify dfs storage rpc server"
    )]
    #[serde(default)]
    pub rpc_tls_storage_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "DFS_STORAGE_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    #[serde(default)]
    pub rpc_tls_storage_service_domain_name: String,
}

impl DfsStorageConfig {
    pub fn default() -> Self {
        DfsStorageConfig {
            address: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            rpc_tls_storage_server_root_ca_cert: "".to_string(),
            rpc_tls_storage_service_domain_name: "".to_string(),
        }
    }
}

impl fmt::Debug for DfsStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "dfs.storage.address: \"{}\", ", self.address)?;
        write!(f, "}}")
    }
}

/// Storage config group.
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct StorageConfig {
    #[structopt(long, env = DEFAULT_STORAGE_TYPE, default_value = "", help = "Default storage type: dfs|disk|s3")]
    #[serde(default)]
    pub default_storage: String,

    // DFS storage backend config.
    #[structopt(flatten)]
    #[serde(rename = "storage.dfs")]
    pub dfs: DfsStorageConfig,
}

impl StorageConfig {
    pub fn default() -> Self {
        StorageConfig {
            default_storage: "disk".to_string(),
            dfs: DfsStorageConfig::default(),
        }
    }

    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(
            mut_config.storage,
            dfs,
            address,
            String,
            DFS_STORAGE_ADDRESS
        );
        env_helper!(
            mut_config.storage,
            dfs,
            username,
            String,
            DFS_STORAGE_USERNAME
        );
        env_helper!(
            mut_config.storage,
            dfs,
            password,
            String,
            DFS_STORAGE_PASSWORD
        );
        env_helper!(
            mut_config.storage,
            dfs,
            rpc_tls_storage_server_root_ca_cert,
            String,
            DFS_STORAGE_RPC_TLS_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config.storage,
            dfs,
            rpc_tls_storage_service_domain_name,
            String,
            DFS_STORAGE_RPC_TLS_SERVICE_DOMAIN_NAME
        );
    }
}
