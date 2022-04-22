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

/// Log config group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long, default_value_t)]
    pub level: String,

    /// Log file dir
    #[clap(long, default_value_t)]
    pub dir: String,

    /// Log file dir
    #[clap(long)]
    pub query_enabled: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "INFO".to_string(),
            dir: "./_logs".to_string(),
            query_enabled: false,
        }
    }
}
