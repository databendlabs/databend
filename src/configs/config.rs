// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
pub struct Config {
    #[structopt(default_value = "Unknown")]
    pub version: String,

    #[structopt(long, default_value = "debug")]
    pub log_level: String,

    #[structopt(long, default_value = "0")]
    pub num_cpus: u64,

    #[structopt(long, default_value = "127.0.0.1")]
    pub mysql_handler_host: String,

    #[structopt(long, default_value = "3307")]
    pub mysql_handler_port: u64,

    #[structopt(long, default_value = "256")]
    pub mysql_handler_thread_num: u64,

    #[structopt(long, default_value = "127.0.0.1:9090")]
    pub rpc_api_address: String,

    #[structopt(long, default_value = "127.0.0.1:8080")]
    pub admin_api_address: String,

    #[structopt(long, default_value = "127.0.0.1:7070")]
    pub metric_api_address: String,
}

impl Config {
    pub fn create() -> Self {
        let mut cfg = Config::from_args();
        if cfg.num_cpus == 0 {
            cfg.num_cpus = num_cpus::get() as u64;
        }
        cfg.version = include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string();
        cfg
    }
}
