// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::configs::config::Password;
use crate::configs::config::User;
use crate::configs::Config;

// Default.
#[test]
fn test_default_config() -> Result<()> {
    let expect = Config {
        log_level: "debug".to_string(),
        log_dir: "./_logs".to_string(),
        num_cpus: 8,
        mysql_handler_host: "127.0.0.1".to_string(),
        mysql_handler_port: 3307,
        max_active_sessions: 256,
        clickhouse_handler_host: "127.0.0.1".to_string(),
        clickhouse_handler_port: 9000,
        flight_api_address: "127.0.0.1:9090".to_string(),
        http_api_address: "127.0.0.1:8080".to_string(),
        metric_api_address: "127.0.0.1:7070".to_string(),
        store_api_address: "127.0.0.1:9191".to_string(),
        store_api_username: User {
            store_api_username: "root".to_string(),
        },
        store_api_password: Password {
            store_api_password: "root".to_string(),
        },
        config_file: "".to_string(),
        tls_server_cert: "".to_string(),
        tls_server_key: "".to_string(),
    };
    let actual = Config::default();
    assert_eq!(actual, expect);
    Ok(())
}

// From env, defaulting.
#[test]
fn test_env_config() -> Result<()> {
    std::env::set_var("FUSE_QUERY_LOG_LEVEL", "DEBUG");
    std::env::set_var("FUSE_QUERY_MYSQL_HANDLER_HOST", "0.0.0.0");
    std::env::set_var("FUSE_QUERY_MYSQL_HANDLER_PORT", "3306");
    std::env::set_var("FUSE_QUERY_MAX_ACTIVE_SESSIONS", "255");
    std::env::set_var("FUSE_QUERY_CLICKHOUSE_HANDLER_HOST", "1.2.3.4");
    std::env::set_var("FUSE_QUERY_CLICKHOUSE_HANDLER_PORT", "9000");
    std::env::set_var("FUSE_QUERY_FLIGHT_API_ADDRESS", "1.2.3.4:9091");
    std::env::set_var("FUSE_QUERY_HTTP_API_ADDRESS", "1.2.3.4:8081");
    std::env::set_var("FUSE_QUERY_METRIC_API_ADDRESS", "1.2.3.4:7071");
    std::env::set_var("STORE_API_ADDRESS", "1.2.3.4:1234");
    std::env::set_var("STORE_API_USERNAME", "admin");
    std::env::set_var("STORE_API_PASSWORD", "password!");
    std::env::remove_var("CONFIG_FILE");
    let default = Config::default();
    let configured = Config::load_from_env(&default)?;
    assert_eq!("DEBUG", configured.log_level);
    assert_eq!("0.0.0.0", configured.mysql_handler_host);
    assert_eq!(3306, configured.mysql_handler_port);
    assert_eq!(255, configured.max_active_sessions);
    assert_eq!("1.2.3.4", configured.clickhouse_handler_host);
    assert_eq!(9000, configured.clickhouse_handler_port);

    assert_eq!("1.2.3.4:9091", configured.flight_api_address);
    assert_eq!("1.2.3.4:8081", configured.http_api_address);
    assert_eq!("1.2.3.4:7071", configured.metric_api_address);

    assert_eq!("1.2.3.4:1234", configured.store_api_address);
    assert_eq!("admin", configured.store_api_username.to_string());
    assert_eq!("password!", configured.store_api_password.to_string());

    // clean up
    std::env::remove_var("FUSE_QUERY_LOG_LEVEL");
    std::env::remove_var("FUSE_QUERY_MYSQL_HANDLER_HOST");
    std::env::remove_var("FUSE_QUERY_MYSQL_HANDLER_PORT");
    std::env::remove_var("FUSE_QUERY_MYSQL_HANDLER_THREAD_NUM");
    std::env::remove_var("FUSE_QUERY_CLICKHOUSE_HANDLER_HOST");
    std::env::remove_var("FUSE_QUERY_CLICKHOUSE_HANDLER_PORT");
    std::env::remove_var("FUSE_QUERY_CLICKHOUSE_HANDLER_THREAD_NUM");
    std::env::remove_var("FUSE_QUERY_FLIGHT_API_ADDRESS");
    std::env::remove_var("FUSE_QUERY_HTTP_API_ADDRESS");
    std::env::remove_var("FUSE_QUERY_METRIC_API_ADDRESS");
    std::env::remove_var("STORE_API_ADDRESS");
    std::env::remove_var("STORE_API_USERNAME");
    std::env::remove_var("STORE_API_PASSWORD");
    Ok(())
}

// From Args.
#[test]
#[ignore]
fn test_args_config() -> Result<()> {
    let actual = Config::load_from_args();
    assert_eq!("INFO", actual.log_level);
    Ok(())
}

// From file NotFound.
#[test]
#[ignore]
fn test_config_file_not_found() -> Result<()> {
    if let Err(e) = Config::load_from_toml("xx.toml") {
        let expect = "Code: 23, displayText = File: xx.toml, err: Os { code: 2, kind: NotFound, message: \"No such file or directory\" }.";
        assert_eq!(expect, format!("{}", e));
    }
    Ok(())
}

// From file.
#[test]
#[ignore]
fn test_file_config() -> Result<()> {
    std::env::set_var("FUSE_QUERY_LOG_LEVEL", "DEBUG");
    let path = std::env::current_dir()
        .unwrap()
        .join("conf/fusequery_config_spec.toml")
        .display()
        .to_string();

    let actual = Config::load_from_toml(path.as_str())?;
    assert_eq!("INFO", actual.log_level);
    let env = Config::load_from_env(&actual)?;
    assert_eq!("INFO", env.log_level);
    std::env::remove_var("FUSE_QUERY_LOG_LEVEL");
    Ok(())
}

// From env, load config file and ignore the rest settings.
#[test]
#[ignore]
fn test_env_file_config() -> Result<()> {
    std::env::set_var("FUSE_QUERY_LOG_LEVEL", "DEBUG");
    let config_path = std::env::current_dir()
        .unwrap()
        .join("conf/fusequery_config_spec.toml")
        .display()
        .to_string();
    std::env::set_var("CONFIG_FILE", config_path);
    let config = Config::load_from_env(&Config::default())?;
    assert_eq!(config.log_level, "INFO");
    std::env::remove_var("FUSE_QUERY_LOG_LEVEL");
    std::env::remove_var("CONFIG_FILE");
    Ok(())
}

#[test]
fn test_fuse_commit_version() -> Result<()> {
    let v = &crate::configs::config::FUSE_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}
