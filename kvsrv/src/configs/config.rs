// Copyright 2020 Datafuse Labs.
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

use common_raft_store::config::RaftConfig;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct Config {
    #[structopt(long, env = "KVSRV_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, env = "KVSRV_LOG_DIR", default_value = "./_logs")]
    pub log_dir: String,

    #[structopt(
        long,
        env = "KVSRV_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:28001"
    )]
    pub metric_api_address: String,

    #[structopt(long, env = "ADMIN_API_ADDRESS", default_value = "127.0.0.1:28002")]
    pub admin_api_address: String,

    #[structopt(long, env = "ADMIN_TLS_SERVER_CERT", default_value = "")]
    pub admin_tls_server_cert: String,

    #[structopt(long, env = "ADMIN_TLS_SERVER_KEY", default_value = "")]
    pub admin_tls_server_key: String,

    #[structopt(
        long,
        env = "KVSRV_FLIGHT_API_ADDRESS",
        default_value = "127.0.0.1:28003"
    )]
    pub flight_api_address: String,

    #[structopt(
        long,
        env = "FLIGHT_TLS_SERVER_CERT",
        default_value = "",
        help = "Certificate for server to identify itself"
    )]
    pub flight_tls_server_cert: String,

    #[structopt(long, env = "FLIGHT_TLS_SERVER_KEY", default_value = "")]
    pub flight_tls_server_key: String,

    #[structopt(flatten)]
    pub raft_config: RaftConfig,
}

impl Config {
    /// StructOptToml provides a default Default impl that loads config from cli args,
    /// which conflicts with unit test if case-filter arguments passed, e.g.:
    /// `cargo test my_unit_test_fn`
    ///
    /// Thus we need another method to generate an empty default instance.
    pub fn empty() -> Self {
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.flight_tls_server_key.is_empty() && !self.flight_tls_server_cert.is_empty()
    }
}
