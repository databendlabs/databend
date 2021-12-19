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

use clap::Args;
use serde::Deserialize;
use serde::Serialize;

use crate::configs::Config;

// Log env.
pub const LOG_LEVEL: &str = "LOG_LEVEL";
pub const LOG_DIR: &str = "LOG_DIR";

/// Log config group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]

pub struct LogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long, env = LOG_LEVEL, default_value = "INFO")]
    pub log_level: String,

    /// Log file dir
    #[clap(required = false, long, env = LOG_DIR, default_value = "./_logs")]
    pub log_dir: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            log_level: "INFO".to_string(),
            log_dir: "./_logs".to_string(),
        }
    }
}

impl LogConfig {
    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, log, log_level, String, LOG_LEVEL);
        env_helper!(mut_config, log, log_dir, String, LOG_DIR);
    }
}
