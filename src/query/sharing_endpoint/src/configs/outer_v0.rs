// Copyright 2021 Datafuse Labs
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
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_self;

use super::inner::Config as InnerConfig;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Parser)]
#[clap(name = "open-sharing", about, version = &**DATABEND_COMMIT_VERSION, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, default_value = "")]
    pub tenant: String,
    #[clap(long, default_value = "127.0.0.1:13003")]
    pub share_endpoint_address: String,
    // Storage backend config.
    #[clap(flatten)]
    pub storage: databend_common_config::StorageConfig,
}

impl Default for Config {
    fn default() -> Self {
        InnerConfig::default().into_outer()
    }
}

impl From<Config> for InnerConfig {
    fn from(x: Config) -> Self {
        InnerConfig {
            tenant: x.tenant,
            share_endpoint_address: x.share_endpoint_address,
            storage: x.storage.try_into().expect("StorageConfig"),
        }
    }
}

impl From<InnerConfig> for Config {
    fn from(inner: InnerConfig) -> Self {
        Self {
            tenant: inner.tenant,
            share_endpoint_address: inner.share_endpoint_address,
            storage: inner.storage.into(),
        }
    }
}

impl Config {
    /// Load will load config from file, env and args.
    ///
    /// - Load from file as default.
    /// - Load from env, will override config from file.
    /// - Load from args as finally override
    ///
    /// # Notes
    ///
    /// with_args is to control whether we need to load from args or not.
    /// We should set this to false during tests because we don't want
    /// our test binary to parse cargo's args.
    pub fn load(with_args: bool) -> Result<Self> {
        let mut arg_conf = Self::default();

        if with_args {
            arg_conf = Self::parse();
        }

        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // load from env.
        builder = builder.collect(from_env());

        // Finally, load from args.
        if with_args {
            builder = builder.collect(from_self(arg_conf));
        }

        Ok(builder.build()?)
    }
}
