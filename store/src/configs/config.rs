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

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct Config {
    #[structopt(long, env = "STORE_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, env = "STORE_LOG_DIR", default_value = "./_logs")]
    pub log_dir: String,

    #[structopt(
        long,
        env = "STORE_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:7171"
    )]
    pub metric_api_address: String,

    #[structopt(long, env = "HTTP_API_ADDRESS", default_value = "127.0.0.1:8181")]
    pub http_api_address: String,

    #[structopt(long, env = "TLS_SERVER_CERT", default_value = "")]
    pub tls_server_cert: String,

    #[structopt(long, env = "TLS_SERVER_KEY", default_value = "")]
    pub tls_server_key: String,

    #[structopt(
        long,
        env = "STORE_FLIGHT_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub flight_api_address: String,

    #[structopt(
        long,
        env = "RPC_TLS_SERVER_CERT",
        default_value = "",
        help = "Certificate for server to identify itself"
    )]
    pub rpc_tls_server_cert: String,

    #[structopt(long, env = "RPC_TLS_SERVER_KEY", default_value = "")]
    pub rpc_tls_server_key: String,

    /// Config for the embedded metasrv.
    #[structopt(flatten)]
    pub meta_config: RaftConfig,

    #[structopt(
        long,
        env = "STORE_LOCAL_FS_DIR",
        help = "Dir for local fs storage",
        default_value = "./_local_fs"
    )]
    pub local_fs_dir: String,
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

    pub fn check(&self) -> common_exception::Result<()> {
        self.meta_config.check()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.rpc_tls_server_key.is_empty() && !self.rpc_tls_server_cert.is_empty()
    }
}
