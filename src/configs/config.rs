// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
pub struct Config {
    #[structopt(default_value = "Unknown")]
    pub version: String,

    #[structopt(default_value = "debug")]
    pub log_level: String,

    #[structopt(default_value = "4")]
    pub num_cpus: u64,

    #[structopt(default_value = "127.0.0.1")]
    pub mysql_listen_host: String,

    #[structopt(default_value = "3307")]
    pub mysql_handler_port: u64,

    #[structopt(default_value = "256")]
    pub mysql_handler_thread_num: u64,
}

impl Config {
    pub fn create() -> Self {
        let mut cfg = Config::from_args();
        cfg.num_cpus = num_cpus::get() as u64;
        cfg.version = include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string();
        cfg
    }
}
