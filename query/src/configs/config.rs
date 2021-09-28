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

use common_exception::ErrorCode;
use common_exception::Result;
use common_store_api_sdk::RpcClientTlsConfig;
use lazy_static::lazy_static;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::configs::QueryConfig;
use crate::configs::StorageConfig;

lazy_static! {
    pub static ref DATABEND_COMMIT_VERSION: String = {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

        let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
            #[cfg(not(feature = "simd"))]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
            #[cfg(feature = "simd")]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => {
                format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
            }
            _ => String::new(),
        };
        ver
    };
}

// Log env.
const LOG_LEVEL: &str = "LOG_LEVEL";
const LOG_DIR: &str = "LOG_DIR";

// Meta env.
const META_ADDRESS: &str = "META_ADDRESS";
const META_USERNAME: &str = "META_USERNAME";
const META_PASSWORD: &str = "META_PASSWORD";
const META_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "META_RPC_TLS_SERVER_ROOT_CA_CERT";
const META_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "META_RPC_TLS_SERVICE_DOMAIN_NAME";

// Config file.
const CONFIG_FILE: &str = "CONFIG_FILE";

/// Log config group.
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct LogConfig {
    #[structopt(long, env = LOG_LEVEL, default_value = "INFO" , help = "Log level <DEBUG|INFO|ERROR>")]
    #[serde(default)]
    pub log_level: String,

    #[structopt(required = false, long, env = LOG_DIR, default_value = "./_logs", help = "Log file dir")]
    #[serde(default)]
    pub log_dir: String,
}

impl LogConfig {
    pub fn default() -> Self {
        LogConfig {
            log_level: "INFO".to_string(),
            log_dir: "./_logs".to_string(),
        }
    }
}

/// Meta config group.
/// serde(default) make the toml de to default working.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct MetaConfig {
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
            meta_address: "".to_string(),
            meta_username: "root".to_string(),
            meta_password: "".to_string(),
            rpc_tls_meta_server_root_ca_cert: "".to_string(),
            rpc_tls_meta_service_domain_name: "localhost".to_string(),
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

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
#[serde(default)]
pub struct Config {
    #[structopt(flatten)]
    pub log: LogConfig,

    // Meta Service config.
    #[structopt(flatten)]
    pub meta: MetaConfig,

    // Storage backend config.
    #[structopt(flatten)]
    pub storage: StorageConfig,

    // Query engine config.
    #[structopt(flatten)]
    pub query: QueryConfig,

    #[structopt(long, short = "c", env = CONFIG_FILE, default_value = "")]
    pub config_file: String,
}

impl Config {
    /// Default configs.
    pub fn default() -> Self {
        Config {
            log: LogConfig::default(),
            meta: MetaConfig::default(),
            storage: StorageConfig::default(),
            query: QueryConfig::default(),
            config_file: "".to_string(),
        }
    }

    /// Load configs from args.
    pub fn load_from_args() -> Self {
        let mut cfg = Config::from_args();
        if cfg.query.num_cpus == 0 {
            cfg.query.num_cpus = num_cpus::get() as u64;
        }
        cfg
    }

    /// Load configs from toml file.
    pub fn load_from_toml(file: &str) -> Result<Self> {
        let txt = std::fs::read_to_string(file)
            .map_err(|e| ErrorCode::CannotReadFile(format!("File: {}, err: {:?}", file, e)))?;
        Self::load_from_toml_str(txt.as_str())
    }

    /// Load configs from toml str.
    pub fn load_from_toml_str(toml_str: &str) -> Result<Self> {
        let mut cfg = Config::from_args_with_toml(toml_str)
            .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e)))?;
        if cfg.query.num_cpus == 0 {
            cfg.query.num_cpus = num_cpus::get() as u64;
        }
        Ok(cfg)
    }

    /// Change config based on configured env variable
    pub fn load_from_env(cfg: &Config) -> Result<Self> {
        let mut mut_config = cfg.clone();
        if std::env::var_os(CONFIG_FILE).is_some() {
            return Config::load_from_toml(
                std::env::var_os(CONFIG_FILE).unwrap().to_str().unwrap(),
            );
        }
        env_helper!(mut_config, log, log_level, String, LOG_LEVEL);

        // Meta.
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

        // Storage.
        StorageConfig::load_from_env(&mut mut_config);

        // Query.
        QueryConfig::load_from_env(&mut mut_config);

        Ok(mut_config)
    }

    pub fn tls_query_client_conf(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.query.rpc_tls_query_server_root_ca_cert.to_string(),
            domain_name: self.query.rpc_tls_query_service_domain_name.to_string(),
        }
    }

    pub fn tls_query_cli_enabled(&self) -> bool {
        !self.query.rpc_tls_query_server_root_ca_cert.is_empty()
            && !self.query.rpc_tls_query_service_domain_name.is_empty()
    }

    pub fn tls_meta_cli_enabled(&self) -> bool {
        !self.meta.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.meta.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.query.rpc_tls_server_key.is_empty() && !self.query.rpc_tls_server_cert.is_empty()
    }
}
