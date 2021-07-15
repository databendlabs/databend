// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::str::FromStr;

use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::Address;
use common_management::cluster::ClusterExecutor;
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

const LOG_LEVEL: &str = "FUSE_QUERY_LOG_LEVEL";
const LOG_DIR: &str = "FUSE_QUERY_LOG_DIR";
const NUM_CPUS: &str = "FUSE_QUERY_NUM_CPUS";

// MySQL.
const MYSQL_HANDLER_HOST: &str = "FUSE_QUERY_MYSQL_HANDLER_HOST";
const MYSQL_HANDLER_PORT: &str = "FUSE_QUERY_MYSQL_HANDLER_PORT";
const MYSQL_HANDLER_THREAD_NUM: &str = "FUSE_QUERY_MYSQL_HANDLER_THREAD_NUM";

// ClickHouse.
const CLICKHOUSE_HANDLER_HOST: &str = "FUSE_QUERY_CLICKHOUSE_HANDLER_HOST";
const CLICKHOUSE_HANDLER_PORT: &str = "FUSE_QUERY_CLICKHOUSE_HANDLER_PORT";
const CLICKHOUSE_HANDLER_THREAD_NUM: &str = "FUSE_QUERY_CLICKHOUSE_HANDLER_THREAD_NUM";

// API
const FLIGHT_API_ADDRESS: &str = "FUSE_QUERY_FLIGHT_API_ADDRESS";
const HTTP_API_ADDRESS: &str = "FUSE_QUERY_HTTP_API_ADDRESS";
const METRICS_API_ADDRESS: &str = "FUSE_QUERY_METRIC_API_ADDRESS";

// Store.
const STORE_API_ADDRESS: &str = "STORE_API_ADDRESS";
const STORE_API_USERNAME: &str = "STORE_API_USERNAME";
const STORE_API_PASSWORD: &str = "STORE_API_PASSWORD";

// Cluster.
const CLUSTER_NAMESPACE: &str = "CLUSTER_NAMESPACE";
const CLUSTER_REGISTRY_URI: &str = "CLUSTER_REGISTRY_URI";
const CLUSTER_EXECUTOR_NAME: &str = "CLUSTER_EXECUTOR_NAME";
const CLUSTER_EXECUTOR_PRIORITY: &str = "CLUSTER_EXECUTOR_PRIORITY";

const CONFIG_FILE: &str = "CONFIG_FILE";

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
    env = MYSQL_HANDLER_THREAD_NUM,
    default_value = "256"
    )]
    pub mysql_handler_thread_num: u64,

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
    pub clickhouse_handler_port: u64,

    #[structopt(
    long,
    env = CLICKHOUSE_HANDLER_THREAD_NUM,
    default_value = "256"
    )]
    pub clickhouse_handler_thread_num: u64,

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

    // Namespace.
    #[structopt(long, env = CLUSTER_NAMESPACE, default_value = "namespace_", help = "Namespace of this executor\n")]
    pub cluster_namespace: String,

    #[structopt(long, env = CLUSTER_REGISTRY_URI, default_value = "http://127.0.0.1:8080", help = "Cluster registry center URI, 'http://':fuse-query, 'local://': local sled, 'store://': fuse-store\n")]
    pub cluster_registry_uri: String,

    #[structopt(long, env = CLUSTER_EXECUTOR_NAME, default_value = "executor_", help = "Executor unique name in the namespace\n")]
    pub cluster_executor_name: String,

    #[structopt(long, env = CLUSTER_EXECUTOR_PRIORITY, default_value = "0")]
    pub cluster_executor_priority: u8,

    #[structopt(long, short = "c", env = CONFIG_FILE, default_value = "")]
    pub config_file: String,
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
            mysql_handler_thread_num: 256,
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,
            clickhouse_handler_thread_num: 256,
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
            cluster_namespace: "n1".to_string(),
            cluster_registry_uri: "http://127.0.0.1:8080".to_string(),
            cluster_executor_name: "".to_string(),
            cluster_executor_priority: 0,
            config_file: "".to_string(),
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

        // MySQL.
        env_helper!(mut_config, mysql_handler_host, String, MYSQL_HANDLER_HOST);
        env_helper!(mut_config, mysql_handler_port, u16, MYSQL_HANDLER_PORT);
        env_helper!(
            mut_config,
            mysql_handler_thread_num,
            u64,
            MYSQL_HANDLER_THREAD_NUM
        );

        // ClickHouse.
        env_helper!(
            mut_config,
            clickhouse_handler_host,
            String,
            CLICKHOUSE_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            clickhouse_handler_port,
            u64,
            CLICKHOUSE_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            clickhouse_handler_thread_num,
            u64,
            CLICKHOUSE_HANDLER_THREAD_NUM
        );

        // API.
        env_helper!(mut_config, flight_api_address, String, FLIGHT_API_ADDRESS);
        env_helper!(mut_config, http_api_address, String, HTTP_API_ADDRESS);
        env_helper!(mut_config, metric_api_address, String, METRICS_API_ADDRESS);

        // Store.
        env_helper!(mut_config, store_api_address, String, STORE_API_ADDRESS);
        env_helper!(mut_config, store_api_username, User, STORE_API_USERNAME);
        env_helper!(mut_config, store_api_password, Password, STORE_API_PASSWORD);

        // Cluster.
        env_helper!(mut_config, cluster_namespace, String, CLUSTER_NAMESPACE);
        env_helper!(
            mut_config,
            cluster_registry_uri,
            String,
            CLUSTER_REGISTRY_URI
        );

        // Executor.
        env_helper!(
            mut_config,
            cluster_executor_name,
            String,
            CLUSTER_EXECUTOR_NAME
        );
        env_helper!(
            mut_config,
            cluster_executor_priority,
            u8,
            CLUSTER_EXECUTOR_PRIORITY
        );

        Ok(mut_config)
    }

    pub fn executor_from_config(&self) -> Result<ClusterExecutor> {
        // Executor using Flight API.
        ClusterExecutor::create(
            self.cluster_executor_name.clone(),
            self.cluster_executor_priority,
            Address::create(self.flight_api_address.as_str())?,
        )
    }
}
