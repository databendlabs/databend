// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::io::Write;

use clap::Parser;
use databend_meta::configs::config as meta_config;
use databend_meta::configs::Config;
use tempfile::tempdir;

#[test]
fn test_tls_rpc_enabled() -> anyhow::Result<()> {
    let mut conf = Config::empty();
    assert!(!conf.tls_rpc_server_enabled());
    conf.grpc_tls_server_key = "test".to_owned();
    assert!(!conf.tls_rpc_server_enabled());
    conf.grpc_tls_server_cert = "test".to_owned();
    assert!(conf.tls_rpc_server_enabled());
    Ok(())
}

#[test]
fn test_load_config() -> anyhow::Result<()> {
    let cfg = Config::try_parse_from(vec!["test", "-c", "foo.toml"]).unwrap();
    assert_eq!("foo.toml", cfg.config_file.as_str());

    let cfg = Config::try_parse_from(vec!["test", "--config-file", "bar.toml"]).unwrap();
    assert_eq!("bar.toml", cfg.config_file.as_str());

    let d = tempdir()?;
    let file_path = d.path().join("foo.toml");
    let mut file = File::create(&file_path)?;
    write!(
        file,
        r#"
log_level = "ERROR"
log_dir = "foo/logs"
metric_api_address = "127.0.0.1:8000"
admin_api_address = "127.0.0.1:9000"
admin_tls_server_cert = "admin tls cert"
admin_tls_server_key = "admin tls key"
grpc_api_address = "127.0.0.1:10000"
grpc_tls_server_cert = "grpc server cert"
grpc_tls_server_key = "grpc server key"

[raft_config]
config_id = "raft config id"
raft_api_host = "127.0.0.1"
raft_listen_host = "127.0.0.1"
raft_api_port = 11000
raft_dir = "raft dir"
no_sync = true
snapshot_logs_since_last = 1000
heartbeat_interval = 2000
install_snapshot_timeout = 3000
single = false
join = ["j1", "j2"]
id = 20
sled_tree_prefix = "sled_foo"
             "#
    )?;

    let mut cfg = Config::load_from_toml(file_path.to_str().unwrap())?;
    assert_eq!(cfg.log_level, "ERROR");
    assert_eq!(cfg.log_dir, "foo/logs");
    assert_eq!(cfg.metric_api_address, "127.0.0.1:8000");
    assert_eq!(cfg.admin_api_address, "127.0.0.1:9000");
    assert_eq!(cfg.admin_tls_server_cert, "admin tls cert");
    assert_eq!(cfg.admin_tls_server_key, "admin tls key");
    assert_eq!(cfg.grpc_api_address, "127.0.0.1:10000");
    assert_eq!(cfg.grpc_tls_server_cert, "grpc server cert");
    assert_eq!(cfg.grpc_tls_server_key, "grpc server key");
    assert_eq!(cfg.raft_config.config_id, "raft config id");
    assert_eq!(cfg.raft_config.raft_listen_host, "127.0.0.1");
    assert_eq!(cfg.raft_config.raft_api_port, 11000);
    assert_eq!(cfg.raft_config.raft_dir, "raft dir");
    assert!(cfg.raft_config.no_sync);
    assert_eq!(cfg.raft_config.snapshot_logs_since_last, 1000);
    assert_eq!(cfg.raft_config.heartbeat_interval, 2000);
    assert_eq!(cfg.raft_config.install_snapshot_timeout, 3000);
    assert!(!cfg.raft_config.single);
    assert_eq!(cfg.raft_config.join, vec!["j1", "j2"]);
    assert_eq!(cfg.raft_config.id, 20);
    assert_eq!(cfg.raft_config.sled_tree_prefix, "sled_foo");

    // test overwrite configs with environment variables
    std::env::set_var(meta_config::METASRV_LOG_LEVEL, "INFO");

    Config::load_from_env(&mut cfg);
    assert_eq!(cfg.log_level, "INFO");
    std::env::remove_var(meta_config::METASRV_LOG_LEVEL);

    Ok(())
}
