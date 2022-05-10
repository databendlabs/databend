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

use std::env;

use clap::Parser;
use common_configs::LogConfig;
use common_configs::MetaConfig;
use common_configs::QueryConfig;
use common_configs::StorageConfig;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, short = 'c', default_value_t)]
    pub config_file: String,

    // Query engine config.
    #[clap(flatten)]
    pub query: QueryConfig,

    #[clap(flatten)]
    pub log: LogConfig,

    // Meta Service config.
    #[clap(flatten)]
    pub meta: MetaConfig,

    // Storage backend config.
    #[clap(flatten)]
    pub storage: StorageConfig,
}

impl Config {
    /// Load will load config from file, env and args.
    ///
    /// - Load from file as default.
    /// - Load from env, will override config from file.
    /// - Load from args as finally override
    pub fn load() -> Result<Self> {
        let arg_conf: Self = Config::parse();

        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file first.
        {
            let config_file = if !arg_conf.config_file.is_empty() {
                arg_conf.config_file.clone()
            } else if let Ok(path) = env::var("CONFIG_FILE") {
                path
            } else {
                "".to_string()
            };

            builder = builder.collect(from_file(Toml, &config_file));
        }

        // Then, load from env.
        builder = builder.collect(from_env());

        // Finally, load from args.
        builder = builder.collect(from_self(arg_conf));

        Ok(builder.build()?)
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
