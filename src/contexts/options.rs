// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::Settings;
use crate::error::{FuseQueryError, FuseQueryResult};

#[derive(Clone, Debug)]
pub struct Options {
    settings: Settings,
}

impl Options {
    pub fn try_create() -> FuseQueryResult<Options> {
        let settings = Settings::create();
        let options = Options { settings };
        options.initial_settings()?;

        Ok(options)
    }

    apply_macros! { apply_getter_setter_settings, apply_initial_settings, apply_update_settings,
        ("log_level", String, "debug".to_string(), "Log Level".to_string()),
        ("num_cpus", u64, num_cpus::get() as u64, "The numbers of the pc".to_string()),
        ("mysql_listen_host", String, "127.0.0.1".to_string(), "MySQL server bind host".to_string()),
        ("mysql_handler_port", u64, 3307, "MySQL protocol port".to_string()),
        ("mysql_handler_thread_num", u64, 256, "MySQL handler thread pool numbers".to_string())
    }
}
