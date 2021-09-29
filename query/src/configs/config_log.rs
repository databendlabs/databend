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

use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::configs::Config;

// Log env.
const LOG_LEVEL: &str = "LOG_LEVEL";
const LOG_DIR: &str = "LOG_DIR";

/// Log config group.
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct LogConfig {
    #[structopt(long, env = LOG_LEVEL, default_value = "INFO" , help = "Log level <DEBUG|INFO|ERROR>")]
    #[serde(default)]
    pub log_level: String,

    #[structopt(required = false, long, env = LOG_DIR, default_value = "./_logs", help = "Log file dir")]
    #[serde(default)]
    pub log_dir: String,
}

impl LogConfig {
    pub fn default() -> Self {
        LogConfig {
            log_level: "INFO".to_string(),
            log_dir: "./_logs".to_string(),
        }
    }

    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, log, log_level, String, LOG_LEVEL);
        env_helper!(mut_config, log, log_dir, String, LOG_DIR);
    }
}
