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
use std::str::FromStr;

use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::RpcClientTlsConfig;
use lazy_static::lazy_static;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

lazy_static! {
    pub static ref FUSE_COMMIT_VERSION: String = {
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
    ($config:expr, $field:tt, $field_type: ty, $env:expr) => {
        let env_var = std::env::var_os($env)
            .unwrap_or($config.$field.to_string().into())
            .into_string()
            .expect(format!("cannot convert {} to string", $env).as_str());
        $config.$field = env_var
            .parse::<$field_type>()
            .expect(format!("cannot convert {} to {}", $env, stringify!($field_type)).as_str());
    };
}

const LOG_LEVEL: &str = "QUERY_LOG_LEVEL";
const LOG_DIR: &str = "QUERY_LOG_DIR";
const NUM_CPUS: &str = "QUERY_NUM_CPUS";

const MYSQL_HANDLER_HOST: &str = "QUERY_MYSQL_HANDLER_HOST";
const MYSQL_HANDLER_PORT: &str = "QUERY_MYSQL_HANDLER_PORT";
const MAX_ACTIVE_SESSIONS: &str = "QUERY_MAX_ACTIVE_SESSIONS";

const CLICKHOUSE_HANDLER_HOST: &str = "QUERY_CLICKHOUSE_HANDLER_HOST";
const CLICKHOUSE_HANDLER_PORT: &str = "QUERY_CLICKHOUSE_HANDLER_PORT";

const FLIGHT_API_ADDRESS: &str = "QUERY_FLIGHT_API_ADDRESS";
const HTTP_API_ADDRESS: &str = "QUERY_HTTP_API_ADDRESS";
const METRICS_API_ADDRESS: &str = "QUERY_METRIC_API_ADDRESS";

const STORE_API_ADDRESS: &str = "STORE_API_ADDRESS";
const STORE_API_USERNAME: &str = "STORE_API_USERNAME";
const STORE_API_PASSWORD: &str = "STORE_API_PASSWORD";

const API_TLS_SERVER_CERT: &str = "API_TLS_SERVER_CERT";
const API_TLS_SERVER_KEY: &str = "API_TLS_SERVER_KEY";

const DISABLE_REMOTE_CATALOG: &str = "DISABLE_REMOTE_CATALOG";

const CONFIG_FILE: &str = "CONFIG_FILE";

const RPC_TLS_SERVER_CERT: &str = "RPC_TLS_SERVER_CERT";
const RPC_TLS_SERVER_KEY: &str = "RPC_TLS_SERVER_KEY";
const RPC_TLS_QUERY_SERVER_ROOT_CA_CERT: &str = "RPC_TLS_QUERY_SERVER_ROOT_CA_CERT";
const RPC_TLS_QUERY_SERVICE_DOMAIN_NAME: &str = "RPC_TLS_QUERY_SERVICE_DOMAIN_NAME";
const RPC_TLS_STORE_SERVER_ROOT_CA_CERT: &str = "RPC_TLS_STORE_SERVER_ROOT_CA_CERT";
const RPC_TLS_STORE_SERVICE_DOMAIN_NAME: &str = "RPC_TLS_STORE_SERVICE_DOMAIN_NAME";

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Config {
    #[structopt(long, env = LOG_LEVEL, default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, env = LOG_DIR, default_value = "./_logs")]
    pub log_dir: String,

    #[structopt(long, env = NUM_CPUS, default_value = "0")]
    pub num_cpus: u64,

    #[structopt(
    long,
    env = MYSQL_HANDLER_HOST,
    default_value = "127.0.0.1"
    )]
    pub mysql_handler_host: String,

    #[structopt(long, env = MYSQL_HANDLER_PORT, default_value = "3307")]
    pub mysql_handler_port: u16,

    #[structopt(
    long,
    env = MAX_ACTIVE_SESSIONS,
    default_value = "256"
    )]
    pub max_active_sessions: u64,

    #[structopt(
    long,
    env = CLICKHOUSE_HANDLER_HOST,
    default_value = "127.0.0.1"
    )]
    pub clickhouse_handler_host: String,

    #[structopt(
    long,
    env = CLICKHOUSE_HANDLER_PORT,
    default_value = "9000"
    )]
    pub clickhouse_handler_port: u16,

    #[structopt(
    long,
    env = FLIGHT_API_ADDRESS,
    default_value = "127.0.0.1:9090"
    )]
    pub flight_api_address: String,

    #[structopt(
    long,
    env = HTTP_API_ADDRESS,
    default_value = "127.0.0.1:8080"
    )]
    pub http_api_address: String,

    #[structopt(
    long,
    env = METRICS_API_ADDRESS,
    default_value = "127.0.0.1:7070"
    )]
    pub metric_api_address: String,

    #[structopt(long, env = STORE_API_ADDRESS, default_value = "127.0.0.1:9191")]
    pub store_api_address: String,

    #[structopt(long, env = STORE_API_USERNAME, default_value = "root")]
    pub store_api_username: User,

    #[structopt(long, env = STORE_API_PASSWORD, default_value = "root")]
    pub store_api_password: Password,

    #[structopt(long, short = "c", env = CONFIG_FILE, default_value = "")]
    pub config_file: String,

    #[structopt(long, env = API_TLS_SERVER_CERT, default_value = "")]
    pub api_tls_server_cert: String,

    #[structopt(long, env = API_TLS_SERVER_KEY, default_value = "")]
    pub api_tls_server_key: String,

    #[structopt(
        long,
        env = "RPC_TLS_SERVER_CERT",
        default_value = "",
        help = "rpc server cert"
    )]
    pub rpc_tls_server_cert: String,

    #[structopt(
        long,
        env = "RPC_TLS_SERVER_KEY",
        default_value = "key for rpc server cert"
    )]
    pub rpc_tls_server_key: String,

    #[structopt(
        long,
        env = "RPC_TLS_QUERY_SERVER_ROOT_CA_CERT",
        default_value = "",
        help = "Certificate for client to identify query rpc server"
    )]
    pub rpc_tls_query_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "RPC_TLS_QUERY_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    pub rpc_tls_query_service_domain_name: String,

    #[structopt(
        long,
        env = "RPC_TLS_STORE_SERVER_ROOT_CA_CERT",
        default_value = "",
        help = "Certificate for client to identify store rpc server"
    )]
    pub rpc_tls_store_server_root_ca_cert: String,

    #[structopt(
        long,
        env = "RPC_TLS_STORE_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    pub rpc_tls_store_service_domain_name: String,

    // this flag will be removed when embedded catalog is ready
    #[structopt(long, env =  DISABLE_REMOTE_CATALOG)]
    pub disable_remote_catalog: bool,
}

#[derive(Clone, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Password {
    pub store_api_password: String,
}

impl AsRef<String> for Password {
    fn as_ref(&self) -> &String {
        &self.store_api_password
    }
}

impl FromStr for Password {
    type Err = ErrorCode;
    fn from_str(s: &str) -> common_exception::Result<Self> {
        Ok(Self {
            store_api_password: s.to_string(),
        })
    }
}

impl ToString for Password {
    fn to_string(&self) -> String {
        self.store_api_password.clone()
    }
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "******")
    }
}

#[derive(Clone, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct User {
    pub store_api_username: String,
}

impl ToString for User {
    fn to_string(&self) -> String {
        self.store_api_username.clone()
    }
}

impl fmt::Debug for User {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "******")
    }
}

impl AsRef<String> for User {
    fn as_ref(&self) -> &String {
        &self.store_api_username
    }
}

impl FromStr for User {
    type Err = ErrorCode;
    fn from_str(s: &str) -> common_exception::Result<Self> {
        Ok(Self {
            store_api_username: s.to_string(),
        })
    }
}

impl Config {
    /// Default configs.
    pub fn default() -> Self {
        Config {
            log_level: "debug".to_string(),
            log_dir: "./_logs".to_string(),
            num_cpus: 8,
            mysql_handler_host: "127.0.0.1".to_string(),
            mysql_handler_port: 3307,
            max_active_sessions: 256,
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,
            flight_api_address: "127.0.0.1:9090".to_string(),
            http_api_address: "127.0.0.1:8080".to_string(),
            metric_api_address: "127.0.0.1:7070".to_string(),
            store_api_address: "127.0.0.1:9191".to_string(),
            store_api_username: User {
                store_api_username: "root".to_string(),
            },
            store_api_password: Password {
                store_api_password: "root".to_string(),
            },
            config_file: "".to_string(),
            api_tls_server_cert: "".to_string(),
            api_tls_server_key: "".to_string(),
            rpc_tls_server_cert: "".to_string(),
            rpc_tls_server_key: "".to_string(),
            rpc_tls_query_server_root_ca_cert: "".to_string(),
            rpc_tls_query_service_domain_name: "localhost".to_string(),
            rpc_tls_store_server_root_ca_cert: "".to_string(),
            rpc_tls_store_service_domain_name: "localhost".to_string(),
            disable_remote_catalog: false,
        }
    }

    /// Load configs from args.
    pub fn load_from_args() -> Self {
        let mut cfg = Config::from_args();
        if cfg.num_cpus == 0 {
            cfg.num_cpus = num_cpus::get() as u64;
        }
        cfg
    }

    /// Load configs from toml file.
    pub fn load_from_toml(file: &str) -> Result<Self> {
        let context = std::fs::read_to_string(file)
            .map_err(|e| ErrorCode::CannotReadFile(format!("File: {}, err: {:?}", file, e)))?;
        let mut cfg = Config::from_args_with_toml(context.as_str())
            .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e)))?;
        if cfg.num_cpus == 0 {
            cfg.num_cpus = num_cpus::get() as u64;
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
        env_helper!(mut_config, log_level, String, LOG_LEVEL);
        env_helper!(mut_config, log_dir, String, LOG_DIR);
        env_helper!(mut_config, num_cpus, u64, NUM_CPUS);
        env_helper!(mut_config, mysql_handler_host, String, MYSQL_HANDLER_HOST);
        env_helper!(mut_config, mysql_handler_port, u16, MYSQL_HANDLER_PORT);
        env_helper!(mut_config, max_active_sessions, u64, MAX_ACTIVE_SESSIONS);
        env_helper!(
            mut_config,
            clickhouse_handler_host,
            String,
            CLICKHOUSE_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            clickhouse_handler_port,
            u16,
            CLICKHOUSE_HANDLER_PORT
        );
        env_helper!(mut_config, flight_api_address, String, FLIGHT_API_ADDRESS);
        env_helper!(mut_config, http_api_address, String, HTTP_API_ADDRESS);
        env_helper!(mut_config, metric_api_address, String, METRICS_API_ADDRESS);
        env_helper!(mut_config, store_api_address, String, STORE_API_ADDRESS);
        env_helper!(mut_config, store_api_username, User, STORE_API_USERNAME);
        env_helper!(mut_config, store_api_password, Password, STORE_API_PASSWORD);

        // for api http service
        env_helper!(mut_config, api_tls_server_cert, String, API_TLS_SERVER_CERT);

        env_helper!(mut_config, api_tls_server_key, String, API_TLS_SERVER_KEY);

        // for query rpc server
        env_helper!(mut_config, rpc_tls_server_cert, String, RPC_TLS_SERVER_CERT);

        env_helper!(mut_config, rpc_tls_server_key, String, RPC_TLS_SERVER_KEY);

        // for query rpc client
        env_helper!(
            mut_config,
            rpc_tls_query_server_root_ca_cert,
            String,
            RPC_TLS_QUERY_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config,
            rpc_tls_query_service_domain_name,
            String,
            RPC_TLS_QUERY_SERVICE_DOMAIN_NAME
        );

        // for store rpc client
        env_helper!(
            mut_config,
            rpc_tls_store_server_root_ca_cert,
            String,
            RPC_TLS_STORE_SERVER_ROOT_CA_CERT
        );

        env_helper!(
            mut_config,
            rpc_tls_store_service_domain_name,
            String,
            RPC_TLS_STORE_SERVICE_DOMAIN_NAME
        );

        Ok(mut_config)
    }

    pub fn tls_query_client_conf(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_query_server_root_ca_cert.to_string(),
            domain_name: self.rpc_tls_query_service_domain_name.to_string(),
        }
    }

    pub fn tls_query_cli_enabled(&self) -> bool {
        !self.rpc_tls_query_server_root_ca_cert.is_empty()
            && !self.rpc_tls_query_service_domain_name.is_empty()
    }

    pub fn tls_store_client_conf(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_store_server_root_ca_cert.to_string(),
            domain_name: self.rpc_tls_store_service_domain_name.to_string(),
        }
    }

    pub fn tls_store_cli_enabled(&self) -> bool {
        !self.rpc_tls_store_server_root_ca_cert.is_empty()
            && !self.rpc_tls_store_service_domain_name.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.rpc_tls_server_key.is_empty() && !self.rpc_tls_server_cert.is_empty()
    }
}
