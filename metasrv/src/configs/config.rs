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
use common_meta_raft_store::config::RaftConfig;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;

lazy_static! {
    pub static ref DATABEND_COMMIT_VERSION: String = {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

        let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
            #[cfg(not(feature = "simd"))]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
            #[cfg(feature = "simd")]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => {
                format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
            }
            _ => String::new(),
        };
        ver
    };
}

pub const METASRV_LOG_LEVEL: &str = "METASRV_LOG_LEVEL";
pub const METASRV_LOG_DIR: &str = "METASRV_LOG_DIR";
pub const METASRV_METRIC_API_ADDRESS: &str = "METASRV_METRIC_API_ADDRESS";
pub const ADMIN_API_ADDRESS: &str = "ADMIN_API_ADDRESS";
pub const ADMIN_TLS_SERVER_CERT: &str = "ADMIN_TLS_SERVER_CERT";
pub const ADMIN_TLS_SERVER_KEY: &str = "ADMIN_TLS_SERVER_KEY";
pub const METASRV_FLIGHT_API_ADDRESS: &str = "METASRV_FLIGHT_API_ADDRESS";
pub const FLIGHT_TLS_SERVER_CERT: &str = "FLIGHT_TLS_SERVER_CERT";
pub const FLIGHT_TLS_SERVER_KEY: &str = "FLIGHT_TLS_SERVER_KEY";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
pub struct Config {
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

    #[clap(long, env = METASRV_FLIGHT_API_ADDRESS, default_value = "127.0.0.1:9191")]
    pub flight_api_address: String,

    /// Certificate for server to identify itself
    #[clap(long, env = FLIGHT_TLS_SERVER_CERT, default_value = "")]
    pub flight_tls_server_cert: String,

    #[clap(long, env = FLIGHT_TLS_SERVER_KEY, default_value = "")]
    pub flight_tls_server_key: String,

    #[clap(flatten)]
    pub raft_config: RaftConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: "INFO".to_string(),
            log_dir: "./_logs".to_string(),
            metric_api_address: "127.0.0.1:28001".to_string(),
            admin_api_address: "127.0.0.1:28002".to_string(),
            admin_tls_server_cert: "".to_string(),
            admin_tls_server_key: "".to_string(),
            flight_api_address: "127.0.0.1:9191".to_string(),
            flight_tls_server_cert: "".to_string(),
            flight_tls_server_key: "".to_string(),
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
        !self.flight_tls_server_key.is_empty() && !self.flight_tls_server_cert.is_empty()
    }
}
