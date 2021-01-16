// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::SettingMap;
use crate::error::FuseQueryResult;

#[derive(Clone, Debug)]
pub struct Options {
    settings: SettingMap,
}

impl Options {
    pub fn try_create() -> FuseQueryResult<Options> {
        let settings = SettingMap::create();
        settings.try_set_string("log_level", "debug".to_string(), "Log level")?;
        settings.try_set_u64("num_cpus", num_cpus::get() as u64, "The numbers of the pc")?;
        settings.try_set_string(
            "mysql_listen_host",
            "127.0.0.1".to_string(),
            "MySQL server bind host",
        )?;
        settings.try_set_u64("mysql_handler_port", 3307, "MySQL protocol port")?;
        settings.try_set_u64(
            "mysql_handler_thread_num",
            256,
            "MySQL handler thread pool numbers",
        )?;
        Ok(Options { settings })
    }

    pub fn get_log_level(&self) -> FuseQueryResult<String> {
        let key = "log_level";
        self.settings.try_get_string(key)
    }

    pub fn get_num_cpus(&self) -> FuseQueryResult<u64> {
        let key = "num_cpus";
        self.settings.try_get_u64(key)
    }

    pub fn get_mysql_listen_host(&self) -> FuseQueryResult<String> {
        let key = "mysql_listen_host";
        self.settings.try_get_string(key)
    }

    pub fn get_mysql_handler_port(&self) -> FuseQueryResult<u64> {
        let key = "mysql_handler_port";
        self.settings.try_get_u64(key)
    }

    pub fn get_mysql_handler_thread_num(&self) -> FuseQueryResult<u64> {
        let key = "mysql_handler_thread_num";
        self.settings.try_get_u64(key)
    }
}
