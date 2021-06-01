// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
pub struct Config {
    #[structopt(env = "FUSE_STORE_VERSION", default_value = "Unknown")]
    pub version: String,

    #[structopt(long, env = "FUSE_STORE_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[structopt(
        long,
        env = "FUSE_STORE_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:7171"
    )]
    pub metric_api_address: String,

    #[structopt(
        long,
        env = "FUSE_QUERY_RPC_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub rpc_api_address: String,
}

impl Config {
    /// Default configs.
    pub fn default() -> Self {
        Config {
            version: include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string(),
            log_level: "debug".to_string(),
            metric_api_address: "127.0.0.1:7171".to_string(),
            rpc_api_address: "127.0.0.1:9191".to_string(),
        }
    }

    /// Create configs from args.
    pub fn load_from_args() -> Self {
        let mut cfg = Config::from_args();
        cfg.version = include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string();
        cfg
    }
}
