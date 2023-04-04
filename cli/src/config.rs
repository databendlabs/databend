// Copyright 2023 Datafuse Labs.
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

// Loading from `$HOME/.config/bendsql/config.toml`

use std::{path::Path, time::Duration};

use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    pub connection: Connection,
    pub settings: Settings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub display_pretty_sql: bool,
    pub prompt: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Connection {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub query_timeout: Duration,
    pub tcp_nodelay: bool,
    #[serde(with = "humantime_serde")]
    pub tcp_keepalive: Option<Duration>,
    #[serde(with = "humantime_serde")]
    pub http2_keep_alive_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub keep_alive_timeout: Duration,
    pub keep_alive_while_idle: bool,
}

impl Config {
    pub fn load() -> Self {
        let path = format!(
            "{}/.config/bendsql/config.toml",
            std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
        );

        let path = Path::new(&path);
        if !path.exists() {
            return Self::default();
        }

        match toml::from_str(&std::fs::read_to_string(path).unwrap()) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("Failed to load config: {}, will use default config", e);
                Self::default()
            }
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            display_pretty_sql: true,
            prompt: "bendsql :) ".to_string(),
        }
    }
}

impl Default for Connection {
    fn default() -> Self {
        Connection {
            connect_timeout: Duration::from_secs(20),
            query_timeout: Duration::from_secs(60),
            tcp_nodelay: true,
            tcp_keepalive: Some(Duration::from_secs(3600)),
            http2_keep_alive_interval: Duration::from_secs(300),
            keep_alive_timeout: Duration::from_secs(20),
            keep_alive_while_idle: true,
        }
    }
}
