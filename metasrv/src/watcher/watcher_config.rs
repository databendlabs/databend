//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
use clap::Parser;
use serde::Deserialize;
use serde::Serialize;

pub const METASRV_WATCHER_NOTIFY_INTERNAL: &str = "METASRV_WATCHER_NOTIFY_INTERNAL";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct WatcherConfig {
    /// Notify watcher new events internal(in ms)
    #[clap(long, env = METASRV_WATCHER_NOTIFY_INTERNAL, default_value = "100")]
    pub watcher_notify_internal: u64,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            watcher_notify_internal: 100,
        }
    }
}
