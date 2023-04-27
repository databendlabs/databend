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

use std::{collections::BTreeMap, path::Path};

use anyhow::anyhow;
use anyhow::Result;
use clap::ValueEnum;
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub connection: ConnectionConfig,
    #[serde(default)]
    pub settings: Settings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub display_pretty_sql: bool,
    pub prompt: String,
    pub progress_color: String,

    /// Show progress [bar] when executing queries.
    /// Only works in non-interactive mode.
    pub show_progress: bool,

    /// Output format is set by the flag.
    #[serde(skip)]
    pub output_format: OutputFormat,
    #[serde(skip)]
    pub time: bool,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Deserialize)]
pub enum OutputFormat {
    Table,
    CSV,
    TSV,
    Null,
}

impl Settings {
    pub fn inject_ctrl_cmd(&mut self, cmd_name: &str, cmd_value: &str) -> Result<()> {
        match cmd_name {
            "display_pretty_sql" => self.display_pretty_sql = cmd_value.parse()?,
            "prompt" => self.prompt = cmd_value.to_string(),
            "progress_color" => self.progress_color = cmd_value.to_string(),
            "show_progress" => self.show_progress = cmd_value.parse()?,
            "output_format" => {
                self.output_format = match cmd_value.to_ascii_lowercase().as_str() {
                    "table" => OutputFormat::Table,
                    "csv" => OutputFormat::CSV,
                    "tsv" => OutputFormat::TSV,
                    "null" => OutputFormat::Null,
                    _ => return Err(anyhow!("Unknown output format: {}", cmd_value)),
                }
            }
            "time" => self.time = cmd_value.parse()?,
            _ => return Err(anyhow!("Unknown command: {}", cmd_name)),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: Option<String>,
    pub args: BTreeMap<String, String>,
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
            progress_color: "cyan".to_string(),
            prompt: "{user}@{host}> ".to_string(),
            output_format: OutputFormat::Table,
            show_progress: false,
            time: false,
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8000,
            user: "root".to_string(),
            database: None,
            args: BTreeMap::new(),
        }
    }
}
