// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[derive(Clone, Debug)]
pub struct Options {
    pub log_level: &'static str,
    pub num_cpus: usize,
    pub mysql_handler_port: usize,
    pub mysql_handler_thread_num: usize,
}

impl Options {
    pub fn default() -> Options {
        Options {
            log_level: "debug",
            num_cpus: num_cpus::get(),
            mysql_handler_port: 3307,
            mysql_handler_thread_num: 256,
        }
    }
}
