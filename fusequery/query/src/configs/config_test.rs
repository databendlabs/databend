// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_config() -> common_exception::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::configs::Config;

    // Default.
    {
        let expect = Config {
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
        };
        let actual = Config::default();
        assert_eq!(actual, expect);
    }

    // From Args.
    {
        let actual = Config::load_from_args();
        assert_eq!("INFO", actual.log_level);
    }

    // From file NotFound.
    {
        if let Err(e) = Config::load_from_toml("xx.toml") {
            let expect = "Code: 23, displayText = File: xx.toml, err: Os { code: 2, kind: NotFound, message: \"No such file or directory\" }.";
            assert_eq!(expect, format!("{}", e));
        }
    }

    // From file.
    {
        let path = std::env::current_dir()
            .unwrap()
            .join("conf/fusequery_config_spec.toml")
            .display()
            .to_string();

        let actual = Config::load_from_toml(path.as_str())?;
        assert_eq!("INFO", actual.log_level);
    }

    Ok(())
}
