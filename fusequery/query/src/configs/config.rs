// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use common_exception::Result;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Config {
    #[structopt(env = "FUSE_QUERY_VERSION", default_value = "Unknown")]
    pub version: String,

    #[structopt(long, env = "FUSE_QUERY_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, env = "FUSE_QUERY_NUM_CPUS", default_value = "0")]
    pub num_cpus: u64,

    #[structopt(
        long,
        env = "FUSE_QUERY_MYSQL_HANDLER_HOST",
        default_value = "127.0.0.1"
    )]
    pub mysql_handler_host: String,

    #[structopt(long, env = "FUSE_QUERY_MYSQL_HANDLER_PORT", default_value = "3307")]
    pub mysql_handler_port: u16,

    #[structopt(
        long,
        env = "FUSE_QUERY_MYSQL_HANDLER_THREAD_NUM",
        default_value = "256"
    )]
    pub mysql_handler_thread_num: u64,

    #[structopt(
        long,
        env = "FUSE_QUERY_CLICKHOUSE_HANDLER_HOST",
        default_value = "127.0.0.1"
    )]
    pub clickhouse_handler_host: String,

    #[structopt(
        long,
        env = "FUSE_QUERY_CLICKHOUSE_HANDLER_PORT",
        default_value = "9000"
    )]
    pub clickhouse_handler_port: u64,

    #[structopt(
        long,
        env = "FUSE_QUERY_CLICKHOUSE_HANDLER_THREAD_NUM",
        default_value = "256"
    )]
    pub clickhouse_handler_thread_num: u64,

    #[structopt(
        long,
        env = "FUSE_QUERY_RPC_API_ADDRESS",
        default_value = "127.0.0.1:9090"
    )]
    pub rpc_api_address: String,

    #[structopt(
        long,
        env = "FUSE_QUERY_HTTP_API_ADDRESS",
        default_value = "127.0.0.1:8080"
    )]
    pub http_api_address: String,

    #[structopt(
        long,
        env = "FUSE_QUERY_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:7070"
    )]
    pub metric_api_address: String,

    #[structopt(long, env = "STORE_API_ADDRESS", default_value = "127.0.0.1:9191")]
    pub store_api_address: String,

    #[structopt(long, env = "STORE_API_USERNAME", default_value = "root")]
    pub store_api_username: String,

    #[structopt(long, env = "STORE_API_PASSWORD", default_value = "root")]
    pub store_api_password: String,

    #[structopt(long, short = "c", env = "CONFIG_FILE", default_value = "")]
    pub config_file: String
}

impl Config {
    /// Default configs.
    pub fn default() -> Self {
        Config {
            version: include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string(),
            log_level: "debug".to_string(),
            num_cpus: 8,
            mysql_handler_host: "127.0.0.1".to_string(),
            mysql_handler_port: 3307,
            mysql_handler_thread_num: 256,
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,
            clickhouse_handler_thread_num: 256,
            rpc_api_address: "127.0.0.1:9090".to_string(),
            http_api_address: "127.0.0.1:8080".to_string(),
            metric_api_address: "127.0.0.1:7070".to_string(),
            store_api_address: "127.0.0.1:9191".to_string(),
            store_api_username: "root".to_string(),
            store_api_password: "root".to_string(),
            config_file: "".to_string()
        }
    }

    /// Load configs from args.
    pub fn load_from_args() -> Self {
        let mut cfg = Config::from_args();
        if cfg.num_cpus == 0 {
            cfg.num_cpus = num_cpus::get() as u64;
        }
        cfg.version = include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string();
        cfg
    }

    /// Load configs from toml file.
    pub fn load_from_toml(file: &str) -> Result<Self> {
        let context = std::fs::read_to_string(file)
            .map_err(|e| ErrorCodes::CannotReadFile(format!("File: {}, err: {:?}", file, e)))?;
        let mut cfg = Config::from_args_with_toml(context.as_str())
            .map_err(|e| ErrorCodes::BadArguments(format!("{:?}", e)))?;
        if cfg.num_cpus == 0 {
            cfg.num_cpus = num_cpus::get() as u64;
        }
        cfg.version = include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string();
        Ok(cfg)
    }
}
