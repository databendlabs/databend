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

macro_rules! env_helper {
    ($config:expr,$struct: tt, $field:tt, $field_type: ty, $env:expr) => {
        let env_var = std::env::var_os($env)
            .unwrap_or($config.$struct.$field.to_string().into())
            .into_string()
            .expect(format!("cannot convert {} to string", $env).as_str());
        $config.$struct.$field = env_var
            .parse::<$field_type>()
            .expect(format!("cannot convert {} to {}", $env, stringify!($field_type)).as_str());
    };
}

// Log env.
const LOG_LEVEL: &str = "LOG_LEVEL";
const LOG_DIR: &str = "LOG_DIR";

// Query env.
const QUERY_TENANT: &str = "QUERY_TENANT";
const QUERY_NAMESPACE: &str = "QUERY_NAMESPACE";
const QUERY_NUM_CPUS: &str = "QUERY_NUM_CPUS";
const QUERY_MYSQL_HANDLER_HOST: &str = "QUERY_MYSQL_HANDLER_HOST";
const QUERY_MYSQL_HANDLER_PORT: &str = "QUERY_MYSQL_HANDLER_PORT";
const QUERY_MAX_ACTIVE_SESSIONS: &str = "QUERY_MAX_ACTIVE_SESSIONS";
const QUERY_CLICKHOUSE_HANDLER_HOST: &str = "QUERY_CLICKHOUSE_HANDLER_HOST";
const QUERY_CLICKHOUSE_HANDLER_PORT: &str = "QUERY_CLICKHOUSE_HANDLER_PORT";
const QUERY_FLIGHT_API_ADDRESS: &str = "QUERY_FLIGHT_API_ADDRESS";
const QUERY_HTTP_API_ADDRESS: &str = "QUERY_HTTP_API_ADDRESS";
const QUERY_METRICS_API_ADDRESS: &str = "QUERY_METRIC_API_ADDRESS";
const QUERY_API_TLS_SERVER_CERT: &str = "QUERY_API_TLS_SERVER_CERT";
const QUERY_API_TLS_SERVER_KEY: &str = "QUERY_API_TLS_SERVER_KEY";
const QUERY_API_TLS_SERVER_ROOT_CA_CERT: &str = "QUERY_API_TLS_SERVER_ROOT_CA_CERT";

const QUERY_RPC_TLS_SERVER_CERT: &str = "QUERY_RPC_TLS_SERVER_CERT";
const QUERY_RPC_TLS_SERVER_KEY: &str = "QUERY_RPC_TLS_SERVER_KEY";
const QUERY_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "QUERY_RPC_TLS_SERVER_ROOT_CA_CERT";
const QUERY_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "QUERY_RPC_TLS_SERVICE_DOMAIN_NAME";
const QUERY_DISABLE_LOCAL_DATABASE_ENGINE: &str = "QUERY_DISABLE_LOCAL_DATABASE_ENGINE";

// Meta env.
const META_ADDRESS: &str = "META_ADDRESS";
const META_USERNAME: &str = "META_USERNAME";
const META_PASSWORD: &str = "META_PASSWORD";
const META_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "META_RPC_TLS_SERVER_ROOT_CA_CERT";
const META_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "META_RPC_TLS_SERVICE_DOMAIN_NAME";

// Store env.
const STORE_ADDRESS: &str = "STORE_ADDRESS";
const STORE_USERNAME: &str = "STORE_USERNAME";
const STORE_PASSWORD: &str = "STORE_PASSWORD";
const STORE_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "STORE_RPC_TLS_SERVER_ROOT_CA_CERT";
const STORE_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "STORE_RPC_TLS_SERVICE_DOMAIN_NAME";

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
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
    }
}

/// Store config group.
/// serde(default) make the toml de to default working.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct StoreConfig {
    #[structopt(long, env = STORE_ADDRESS, default_value, help = "Store backend address")]
    #[serde(default)]
    pub store_address: String,

    #[structopt(long, env = STORE_USERNAME, default_value = "root", help = "Store backend user name")]
    #[serde(default)]
    pub store_username: String,

    #[structopt(long, env = STORE_PASSWORD, default_value, help = "Store backend user password")]
    #[serde(default)]
    pub store_password: String,

    #[structopt(
        long,
        env = "STORE_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value,
        help = "Certificate for client to identify store rpc server"
    )]
    #[serde(default)]
    pub rpc_tls_store_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "STORE_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    #[serde(default)]
    pub rpc_tls_store_service_domain_name: String,
}

impl StoreConfig {
    pub fn default() -> Self {
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
    }
}

impl fmt::Debug for StoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "store_address: \"{}\", ", self.store_address)?;
        write!(f, "store_user: \"{}\", ", self.store_username)?;
        write!(f, "store_password: \"******\"")?;
        write!(f, "}}")
    }
}

/// Meta config group.
/// serde(default) make the toml de to default working.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct MetaConfig {
    #[structopt(long, env = META_ADDRESS, default_value, help = "MetaStore backend address")]
    #[serde(default)]
    pub meta_address: String,

    #[structopt(long, env = META_USERNAME, default_value = "root", help = "MetaStore backend user name")]
    #[serde(default)]
    pub meta_username: String,

    #[structopt(long, env = META_PASSWORD, default_value, help = "MetaStore backend user password")]
    #[serde(default)]
    pub meta_password: String,

    #[structopt(
        long,
        env = "META_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value,
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
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
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

/// Query config group.
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct QueryConfig {
    #[structopt(long, env = QUERY_TENANT, default_value, help = "Tenant id for get the information from the MetaStore")]
    #[serde(default)]
    pub tenant: String,

    #[structopt(long, env = QUERY_NAMESPACE, default_value, help = "Namespace for construct the cluster")]
    #[serde(default)]
    pub namespace: String,

    #[structopt(long, env = QUERY_NUM_CPUS, default_value = "8")]
    #[serde(default)]
    pub num_cpus: u64,

    #[structopt(
    long,
    env = QUERY_MYSQL_HANDLER_HOST,
    default_value = "127.0.0.1"
    )]
    #[serde(default)]
    pub mysql_handler_host: String,

    #[structopt(long, env = QUERY_MYSQL_HANDLER_PORT, default_value = "3307")]
    #[serde(default)]
    pub mysql_handler_port: u16,

    #[structopt(
    long,
    env = QUERY_MAX_ACTIVE_SESSIONS,
    default_value = "256"
    )]
    #[serde(default)]
    pub max_active_sessions: u64,

    #[structopt(
    long,
    env = QUERY_CLICKHOUSE_HANDLER_HOST,
    default_value = "127.0.0.1"
    )]
    #[serde(default)]
    pub clickhouse_handler_host: String,

    #[structopt(
    long,
    env = QUERY_CLICKHOUSE_HANDLER_PORT,
    default_value = "9000"
    )]
    #[serde(default)]
    pub clickhouse_handler_port: u16,

    #[structopt(
    long,
    env = QUERY_FLIGHT_API_ADDRESS,
    default_value = "127.0.0.1:9090"
    )]
    #[serde(default)]
    pub flight_api_address: String,

    #[structopt(
    long,
    env = QUERY_HTTP_API_ADDRESS,
    default_value = "127.0.0.1:8080"
    )]
    #[serde(default)]
    pub http_api_address: String,

    #[structopt(
    long,
    env = QUERY_METRICS_API_ADDRESS,
    default_value = "127.0.0.1:7070"
    )]
    #[serde(default)]
    pub metric_api_address: String,

    #[structopt(long, env = QUERY_API_TLS_SERVER_CERT, default_value)]
    #[serde(default)]
    pub api_tls_server_cert: String,

    #[structopt(long, env = QUERY_API_TLS_SERVER_KEY, default_value)]
    #[serde(default)]
    pub api_tls_server_key: String,

    #[structopt(long, env = QUERY_API_TLS_SERVER_ROOT_CA_CERT, default_value)]
    #[serde(default)]
    pub api_tls_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVER_CERT",
        default_value,
        help = "rpc server cert"
    )]
    #[serde(default)]
    pub rpc_tls_server_cert: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVER_KEY",
        default_value,
        help = "key for rpc server cert"
    )]
    #[serde(default)]
    pub rpc_tls_server_key: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVER_ROOT_CA_CERT",
        default_value,
        help = "Certificate for client to identify query rpc server"
    )]
    #[serde(default)]
    pub rpc_tls_query_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "QUERY_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    #[serde(default)]
    pub rpc_tls_query_service_domain_name: String,

    #[structopt(long, env = "QUERY_DISABLE_LOCAL_DATABASE_ENGINE", default_value = "0")]
    #[serde(default)]
    pub disable_local_database_engine: String,
}

impl QueryConfig {
    pub fn default() -> Self {
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
    }
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Config {
    #[structopt(flatten)]
    pub log: LogConfig,

    // Meta Service config.
    #[structopt(flatten)]
    pub meta: MetaConfig,

    // Storage backend config.
    #[structopt(flatten)]
    pub store: StoreConfig,

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
            store: StoreConfig::default(),
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

        // Store.
        env_helper!(mut_config, store, store_address, String, STORE_ADDRESS);
        env_helper!(mut_config, store, store_username, String, STORE_USERNAME);
        env_helper!(mut_config, store, store_password, String, STORE_PASSWORD);

        // for store rpc client
        env_helper!(
            mut_config,
            store,
            rpc_tls_store_server_root_ca_cert,
            String,
            STORE_RPC_TLS_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config,
            store,
            rpc_tls_store_service_domain_name,
            String,
            STORE_RPC_TLS_SERVICE_DOMAIN_NAME
        );

        // Query.
        env_helper!(mut_config, query, tenant, String, QUERY_TENANT);
        env_helper!(mut_config, query, namespace, String, QUERY_NAMESPACE);
        env_helper!(mut_config, query, num_cpus, u64, QUERY_NUM_CPUS);
        env_helper!(
            mut_config,
            query,
            mysql_handler_host,
            String,
            QUERY_MYSQL_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            query,
            mysql_handler_port,
            u16,
            QUERY_MYSQL_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            query,
            max_active_sessions,
            u64,
            QUERY_MAX_ACTIVE_SESSIONS
        );
        env_helper!(
            mut_config,
            query,
            clickhouse_handler_host,
            String,
            QUERY_CLICKHOUSE_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            query,
            clickhouse_handler_port,
            u16,
            QUERY_CLICKHOUSE_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            query,
            flight_api_address,
            String,
            QUERY_FLIGHT_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            http_api_address,
            String,
            QUERY_HTTP_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            metric_api_address,
            String,
            QUERY_METRICS_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            disable_local_database_engine,
            String,
            QUERY_DISABLE_LOCAL_DATABASE_ENGINE
        );

        // for api http service
        env_helper!(
            mut_config,
            query,
            api_tls_server_cert,
            String,
            QUERY_API_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            api_tls_server_key,
            String,
            QUERY_API_TLS_SERVER_KEY
        );

        // for query rpc server
        env_helper!(
            mut_config,
            query,
            rpc_tls_server_cert,
            String,
            QUERY_RPC_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            rpc_tls_server_key,
            String,
            QUERY_RPC_TLS_SERVER_KEY
        );

        // for query rpc client
        env_helper!(
            mut_config,
            query,
            rpc_tls_query_server_root_ca_cert,
            String,
            QUERY_RPC_TLS_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config,
            query,
            rpc_tls_query_service_domain_name,
            String,
            QUERY_RPC_TLS_SERVICE_DOMAIN_NAME
        );

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

    pub fn tls_store_client_conf(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.store.rpc_tls_store_server_root_ca_cert.to_string(),
            domain_name: self.store.rpc_tls_store_service_domain_name.to_string(),
        }
    }

    pub fn tls_store_cli_enabled(&self) -> bool {
        !self.store.rpc_tls_store_server_root_ca_cert.is_empty()
            && !self.store.rpc_tls_store_service_domain_name.is_empty()
    }

    pub fn tls_meta_cli_enabled(&self) -> bool {
        !self.meta.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.meta.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.query.rpc_tls_server_key.is_empty() && !self.query.rpc_tls_server_cert.is_empty()
    }
}
