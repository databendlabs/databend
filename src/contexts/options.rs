// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
pub struct Opt {
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

impl Opt {
    pub fn create() -> Self {
        let mut opt = Opt::from_args();
        opt.num_cpus = num_cpus::get() as u64;
        opt.version = include_str!(concat!(env!("OUT_DIR"), "/version-info.txt")).to_string();
        opt
    }
}
