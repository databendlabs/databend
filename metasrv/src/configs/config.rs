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

use clap::Parser;
use common_meta_raft_store::config as raft_config;
use common_meta_raft_store::config::RaftConfig;
use common_meta_types::MetaError;
use common_meta_types::MetaResult;
use serde::Deserialize;
use serde::Serialize;

macro_rules! load_field_from_env {
    ($field:expr, $field_type: ty, $env:expr) => {
        if let Some(env_var) = std::env::var_os($env) {
            $field = env_var
                .into_string()
                .expect(format!("cannot convert {} to string", $env).as_str())
                .parse::<$field_type>()
                .expect(format!("cannot convert {} to {}", $env, stringify!($field_type)).as_str());
        }
    };
}

pub const METASRV_LOG_LEVEL: &str = "METASRV_LOG_LEVEL";
pub const METASRV_LOG_DIR: &str = "METASRV_LOG_DIR";
pub const METASRV_METRIC_API_ADDRESS: &str = "METASRV_METRIC_API_ADDRESS";
pub const ADMIN_API_ADDRESS: &str = "ADMIN_API_ADDRESS";
pub const ADMIN_TLS_SERVER_CERT: &str = "ADMIN_TLS_SERVER_CERT";
pub const ADMIN_TLS_SERVER_KEY: &str = "ADMIN_TLS_SERVER_KEY";
pub const METASRV_GRPC_API_ADDRESS: &str = "METASRV_GRPC_API_ADDRESS";
pub const GRPC_TLS_SERVER_CERT: &str = "GRPC_TLS_SERVER_CERT";
pub const GRPC_TLS_SERVER_KEY: &str = "GRPC_TLS_SERVER_KEY";

/// METASRV Config file.
const METASRV_CONFIG_FILE: &str = "METASRV_CONFIG_FILE";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, short = 'c', env = METASRV_CONFIG_FILE, default_value = "")]
    pub config_file: String,

    #[clap(long, env = METASRV_LOG_LEVEL, default_value = "INFO")]
    pub log_level: String,

    #[clap(long, env = METASRV_LOG_DIR, default_value = "./_logs")]
    pub log_dir: String,

    #[clap(long, env = METASRV_METRIC_API_ADDRESS, default_value = "127.0.0.1:28001")]
    pub metric_api_address: String,

    #[clap(long, env = ADMIN_API_ADDRESS, default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,

    #[clap(long, env = ADMIN_TLS_SERVER_CERT, default_value = "")]
    pub admin_tls_server_cert: String,

    #[clap(long, env = ADMIN_TLS_SERVER_KEY, default_value = "")]
    pub admin_tls_server_key: String,

    #[clap(long, env = METASRV_GRPC_API_ADDRESS, default_value = "127.0.0.1:9191")]
    pub grpc_api_address: String,

    /// Certificate for server to identify itself
    #[clap(long, env = GRPC_TLS_SERVER_CERT, default_value = "")]
    pub grpc_tls_server_cert: String,

    #[clap(long, env = GRPC_TLS_SERVER_KEY, default_value = "")]
    pub grpc_tls_server_key: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            config_file: "".to_string(),
            log_level: "INFO".to_string(),
            log_dir: "./_logs".to_string(),
            metric_api_address: "127.0.0.1:28001".to_string(),
            admin_api_address: "127.0.0.1:28002".to_string(),
            admin_tls_server_cert: "".to_string(),
            admin_tls_server_key: "".to_string(),
            grpc_api_address: "127.0.0.1:9191".to_string(),
            grpc_tls_server_cert: "".to_string(),
            grpc_tls_server_key: "".to_string(),
            raft_config: Default::default(),
        }
    }
}

impl Config {
    /// StructOptToml provides a default Default impl that loads config from cli args,
    /// which conflicts with unit test if case-filter arguments passed, e.g.:
    /// `cargo test my_unit_test_fn`
    ///
    /// Thus we need another method to generate an empty default instance.
    pub fn empty() -> Self {
        <Self as Parser>::parse_from(&Vec::<&'static str>::new())
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.grpc_tls_server_key.is_empty() && !self.grpc_tls_server_cert.is_empty()
    }

    pub fn check(&self) -> MetaResult<()> {
        self.raft_config.check()?;
        Ok(())
    }

    /// First load configs from args.
    /// If config file is not empty, e.g: `-c xx.toml`, reload config from the file.
    /// Prefer to use environment variables in cloud native deployment.
    /// Override configs base on environment variables.
    pub fn load() -> Result<Self, MetaError> {
        let mut cfg = Config::parse();
        if !cfg.config_file.is_empty() {
            cfg = Self::load_from_toml(cfg.config_file.as_str())?;
        }

        Self::load_from_env(&mut cfg);
        cfg.check()?;
        Ok(cfg)
    }

    /// Load configs from toml file.
    pub fn load_from_toml(file: &str) -> Result<Self, MetaError> {
        let txt = std::fs::read_to_string(file)
            .map_err(|e| MetaError::LoadConfigError(format!("File: {}, err: {:?}", file, e)))?;

        let cfg = toml::from_str::<Config>(txt.as_str())
            .map_err(|e| MetaError::LoadConfigError(format!("{:?}", e)))?;

        cfg.check()?;
        Ok(cfg)
    }

    /// Load configs from environment variables.
    pub fn load_from_env(cfg: &mut Config) {
        load_field_from_env!(cfg.log_level, String, METASRV_LOG_LEVEL);
        load_field_from_env!(cfg.log_dir, String, METASRV_LOG_DIR);
        load_field_from_env!(cfg.metric_api_address, String, METASRV_METRIC_API_ADDRESS);
        load_field_from_env!(cfg.admin_api_address, String, ADMIN_API_ADDRESS);
        load_field_from_env!(cfg.admin_tls_server_cert, String, ADMIN_TLS_SERVER_CERT);
        load_field_from_env!(cfg.admin_tls_server_key, String, ADMIN_TLS_SERVER_KEY);
        load_field_from_env!(cfg.grpc_api_address, String, METASRV_GRPC_API_ADDRESS);
        load_field_from_env!(cfg.grpc_tls_server_cert, String, GRPC_TLS_SERVER_CERT);
        load_field_from_env!(cfg.grpc_tls_server_key, String, GRPC_TLS_SERVER_KEY);
        load_field_from_env!(
            cfg.raft_config.raft_listen_host,
            String,
            raft_config::KVSRV_LISTEN_HOST
        );
        load_field_from_env!(
            cfg.raft_config.raft_advertise_host,
            String,
            raft_config::KVSRV_ADVERTISE_HOST
        );
        load_field_from_env!(
            cfg.raft_config.raft_api_port,
            u32,
            raft_config::KVSRV_API_PORT
        );
        load_field_from_env!(
            cfg.raft_config.raft_dir,
            String,
            raft_config::KVSRV_RAFT_DIR
        );
        load_field_from_env!(cfg.raft_config.no_sync, bool, raft_config::KVSRV_NO_SYNC);
        load_field_from_env!(
            cfg.raft_config.snapshot_logs_since_last,
            u64,
            raft_config::KVSRV_SNAPSHOT_LOGS_SINCE_LAST
        );
        load_field_from_env!(
            cfg.raft_config.heartbeat_interval,
            u64,
            raft_config::KVSRV_HEARTBEAT_INTERVAL
        );
        load_field_from_env!(
            cfg.raft_config.install_snapshot_timeout,
            u64,
            raft_config::KVSRV_INSTALL_SNAPSHOT_TIMEOUT
        );
        load_field_from_env!(cfg.raft_config.single, bool, raft_config::KVSRV_SINGLE);
        load_field_from_env!(cfg.raft_config.id, u64, raft_config::KVSRV_ID);
    }
}
