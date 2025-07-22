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
use clap::Subcommand;
use databend_common_config::Config;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_version::DATABEND_COMMIT_VERSION;

/// Databend
#[derive(Parser, Clone)]
#[command(name = "databend-query")]
#[command(about = "Databend: The Next-Gen Cloud [Data+AI] Analytics.")]
#[command(version = &**DATABEND_COMMIT_VERSION)]
pub struct Cmd {
    /// Run a command and quit
    #[command(subcommand)]
    pub subcommand: Option<Commands>,

    /// To be compatible with the old version, we keep the `cmd` arg
    /// We should always use `databend-query ver` instead `databend-query --cmd ver` in latest version
    #[clap(long)]
    pub cmd: Option<String>,

    #[clap(long, short = 'c', value_name = "PATH", default_value_t)]
    pub config_file: String,

    #[clap(flatten)]
    pub config: Config,
}

impl Cmd {
    pub fn normalize(&mut self) {
        if self.cmd == Some("ver".to_string()) {
            self.subcommand = Some(Commands::Ver);
        }
    }

    pub async fn init_inner_config(self, check_meta: bool) -> Result<InnerConfig> {
        let Cmd {
            config,
            config_file,
            ..
        } = self;

        let config = config.merge(&config_file).unwrap();
        InnerConfig::init(config, check_meta).await
    }
}

#[derive(Subcommand, Clone)]
pub enum Commands {
    #[command(about = "Print version and quit")]
    Ver,
    Local {
        #[clap(long, short = 'q', default_value_t)]
        query: String,
        #[clap(long, default_value_t)]
        output_format: String,
        #[clap(long, short = 'c')]
        config: String,
    },
}
