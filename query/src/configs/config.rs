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
use common_exception::Result;
use common_grpc::RpcClientTlsConfig;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::Environment;
use serfig::collectors::File;
use serfig::parsers::Toml;

use crate::configs::LogConfig;
use crate::configs::MetaConfig;
use crate::configs::QueryConfig;
use crate::configs::StorageConfig;

pub static DATABEND_COMMIT_VERSION: Lazy<String> = Lazy::new(|| {
    let git_tag = option_env!("VERGEN_GIT_SEMVER");
    let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
    let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

    let ver = match (git_tag, git_sha, rustc_semver, timestamp) {
        #[cfg(not(feature = "simd"))]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}(rust-{}-{})", v1, v2, v3, v4),
        #[cfg(feature = "simd")]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => {
            format!("{}-{}-simd(rust-{}-{})", v1, v2, v3, v4)
        }
        _ => String::new(),
    };
    ver
});

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, short = 'c', default_value = "")]
    pub config_file: String,

    // Query engine config.
    #[clap(flatten)]
    pub query: QueryConfig,

    #[clap(flatten)]
    #[serde(flatten)]
    pub log: LogConfig,

    // Meta Service config.
    #[clap(flatten)]
    pub meta: MetaConfig,

    // Storage backend config.
    #[clap(flatten)]
    pub storage: StorageConfig,
}

impl Default for Config {
    /// Default configs.
    fn default() -> Self {
        Self {
            config_file: "".to_string(),
            query: QueryConfig::default(),
            log: LogConfig::default(),
            meta: MetaConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let arg_conf = Config::parse();

        // TODO: load from file first.

        // Load from env first as default.
        let mut builder = serfig::Builder::default().collect(Environment::create());

        // TODO: load from CONFIG_FILE after env.

        // Override by file if exist
        if !arg_conf.config_file.is_empty() {
            builder = builder.collect(File::create(&arg_conf.config_file, Toml));
        }
        // Override by args.
        builder = builder.collect(Box::new(
            serde_bridge::into_value(arg_conf).expect("into value failed"),
        ));

        Ok(builder.build()?)
    }

    pub fn tls_query_client_conf(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.query.rpc_tls_query_server_root_ca_cert.to_string(),
            domain_name: self.query.rpc_tls_query_service_domain_name.to_string(),
        }
    }

    pub fn tls_query_cli_enabled(&self) -> bool {
        !self.query.rpc_tls_query_server_root_ca_cert.is_empty()
            && !self.query.rpc_tls_query_service_domain_name.is_empty()
    }

    pub fn tls_meta_cli_enabled(&self) -> bool {
        !self.meta.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.meta.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.query.rpc_tls_server_key.is_empty() && !self.query.rpc_tls_server_cert.is_empty()
    }
}
