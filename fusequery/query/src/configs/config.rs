// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;
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

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Config {
    #[structopt(long, default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, default_value = "./_logs")]
    pub log_dir: String,

    #[structopt(long, default_value = "0")]
    pub num_cpus: u64,

    #[structopt(long, default_value = "127.0.0.1")]
    pub mysql_handler_host: String,

    #[structopt(long, default_value = "3307")]
    pub mysql_handler_port: u16,

    #[structopt(long, default_value = "256")]
    pub mysql_handler_thread_num: u64,

    #[structopt(long, default_value = "127.0.0.1")]
    pub clickhouse_handler_host: String,

    #[structopt(long, default_value = "9000")]
    pub clickhouse_handler_port: u64,

    #[structopt(long, default_value = "256")]
    pub clickhouse_handler_thread_num: u64,

    #[structopt(long, default_value = "127.0.0.1:9090")]
    pub flight_api_address: String,

    #[structopt(long, default_value = "127.0.0.1:8080")]
    pub http_api_address: String,

    #[structopt(long, default_value = "127.0.0.1:7070")]
    pub metric_api_address: String,

    #[structopt(long, default_value = "127.0.0.1:9191")]
    pub store_api_address: String,

    #[structopt(long, default_value = "root")]
    pub store_api_username: String,

    #[structopt(long, default_value = "root")]
    pub store_api_password: String,

    #[structopt(long, short = "c", default_value = "")]
    pub config_file: String,
}

// adaptor for environment variable
#[derive(serde::Deserialize, Debug)]
struct EnvConfig {
    fuse_query_log_level: Option<String>,
    fuse_query_log_dir: Option<String>,
    fuse_query_num_cpus: Option<u64>,
    fuse_query_mysql_handler_host: Option<String>,
    fuse_query_mysql_handler_port: Option<u16>,
    fuse_query_mysql_handler_thread_num: Option<u64>,
    fuse_query_clickhouse_handler_host: Option<String>,
    fuse_query_clickhouse_handler_port: Option<u64>,
    fuse_query_clickhouse_handler_thread_num: Option<u64>,
    fuse_query_flight_api_address: Option<String>,
    fuse_query_http_api_address: Option<String>,
    fuse_query_metric_api_address: Option<String>,
    store_api_address: Option<String>,
    store_api_username: Option<String>,
    store_api_password: Option<String>,
    config_file: Option<String>,
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
            store_api_username: "root".to_string(),
            store_api_password: "root".to_string(),
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
        let env_config = envy::from_env::<EnvConfig>()
            .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e)))?;
        // prefer to load configuration based on environment variable
        if env_config.config_file.is_some() {
            return Config::load_from_toml(env_config.config_file.unwrap().as_str());
        }
        // log env
        mut_config.log_level = env_config
            .fuse_query_log_level
            .map_or(mut_config.log_level, |v| v);
        mut_config.log_dir = env_config
            .fuse_query_log_dir
            .map_or(mut_config.log_dir, |v| v);

        // mysql client env
        mut_config.mysql_handler_host = env_config
            .fuse_query_mysql_handler_host
            .map_or(mut_config.mysql_handler_host, |v| v);
        mut_config.mysql_handler_port = env_config
            .fuse_query_mysql_handler_port
            .map_or(mut_config.mysql_handler_port, |v| v);
        mut_config.mysql_handler_thread_num = env_config
            .fuse_query_mysql_handler_thread_num
            .map_or(mut_config.mysql_handler_thread_num, |v| v);

        // clickhouse client env
        mut_config.clickhouse_handler_host = env_config
            .fuse_query_clickhouse_handler_host
            .map_or(mut_config.clickhouse_handler_host, |v| v);
        mut_config.clickhouse_handler_port = env_config
            .fuse_query_clickhouse_handler_port
            .map_or(mut_config.clickhouse_handler_port, |v| v);
        mut_config.clickhouse_handler_thread_num = env_config
            .fuse_query_clickhouse_handler_thread_num
            .map_or(mut_config.clickhouse_handler_thread_num, |v| v);

        // rpc and metrics env
        mut_config.http_api_address = env_config
            .fuse_query_http_api_address
            .map_or(mut_config.http_api_address, |v| v);
        mut_config.flight_api_address = env_config
            .fuse_query_flight_api_address
            .map_or(mut_config.flight_api_address, |v| v);
        mut_config.metric_api_address = env_config
            .fuse_query_metric_api_address
            .map_or(mut_config.metric_api_address, |v| v);

        // store env
        mut_config.store_api_address = env_config
            .store_api_address
            .map_or(mut_config.store_api_address, |v| v);
        mut_config.store_api_username = env_config
            .store_api_username
            .map_or(mut_config.store_api_username, |v| v);
        mut_config.store_api_password = env_config
            .store_api_password
            .map_or(mut_config.store_api_password, |v| v);

        // cpu env
        mut_config.num_cpus = env_config
            .fuse_query_num_cpus
            .map_or(mut_config.num_cpus, |v| v);
        Ok(mut_config)
    }
}
