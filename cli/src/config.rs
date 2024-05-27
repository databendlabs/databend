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
    pub settings: SettingsConfig,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(default)]
pub struct SettingsConfig {
    pub display_pretty_sql: Option<bool>,
    pub prompt: Option<String>,
    pub progress_color: Option<String>,
    pub no_auto_complete: Option<bool>,
    pub show_progress: Option<bool>,
    pub show_stats: Option<bool>,
    pub expand: Option<String>,
    pub replace_newline: Option<bool>,
    pub max_display_rows: Option<usize>,
    pub max_col_width: Option<usize>,
    pub max_width: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum ExpandMode {
    On,
    Off,
    Auto,
}

impl From<&str> for ExpandMode {
    fn from(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "on" => ExpandMode::On,
            "off" => ExpandMode::Off,
            "auto" => ExpandMode::Auto,
            _ => ExpandMode::Auto,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub display_pretty_sql: bool,
    pub prompt: String,
    pub progress_color: String,
    pub no_auto_complete: bool,

    /// Show progress [bar] when executing queries.
    /// Only works with output format `table` and `null`.
    pub show_progress: bool,

    /// Show stats after executing queries.
    /// Only works with non-interactive mode.
    pub show_stats: bool,
    /// Output max rows (only works in table output format)
    pub max_display_rows: usize,
    /// limit display render each column max width, smaller than 3 means disable the limit
    pub max_col_width: usize,
    /// limit display render box max width, 0 means default to the size of the terminal
    pub max_width: usize,
    /// Output format is set by the flag.
    pub output_format: OutputFormat,
    // Output Quote Style.
    pub quote_style: OutputQuoteStyle,
    /// Expand table format display, default off, could be on/off/auto.
    /// only works with output format `table`.
    pub expand: ExpandMode,

    /// Show time elapsed when executing queries.
    /// only works with output format `null`.
    pub time: Option<TimeOption>,

    /// Multi line mode, default is true.
    pub multi_line: bool,
    /// whether replace '\n' with '\\n', default true.
    pub replace_newline: bool,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Deserialize)]
pub enum OutputFormat {
    Table,
    CSV,
    TSV,
    Null,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Deserialize)]
pub enum OutputQuoteStyle {
    Always,
    Necessary,
    NonNumeric,
    Never,
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum TimeOption {
    Local,
    Server,
}

impl TryFrom<&str> for TimeOption {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "server" => Ok(TimeOption::Server),
            "local" => Ok(TimeOption::Local),
            _ => Err(anyhow!("Unknown time display option: {}", s)),
        }
    }
}

impl Settings {
    pub fn merge_config(&mut self, cfg: SettingsConfig) {
        self.display_pretty_sql = cfg.display_pretty_sql.unwrap_or(self.display_pretty_sql);
        self.prompt = cfg.prompt.unwrap_or_else(|| self.prompt.clone());
        self.progress_color = cfg
            .progress_color
            .unwrap_or_else(|| self.progress_color.clone());
        self.no_auto_complete = cfg.no_auto_complete.unwrap_or(self.no_auto_complete);
        self.show_progress = cfg.show_progress.unwrap_or(self.show_progress);
        self.show_stats = cfg.show_stats.unwrap_or(self.show_stats);
        self.expand = cfg
            .expand
            .map(|expand| expand.as_str().into())
            .unwrap_or_else(|| self.expand.clone());
        self.replace_newline = cfg.replace_newline.unwrap_or(self.replace_newline);
        self.max_width = cfg.max_width.unwrap_or(self.max_width);
        self.max_col_width = cfg.max_col_width.unwrap_or(self.max_col_width);
        self.max_display_rows = cfg.max_display_rows.unwrap_or(self.max_display_rows);
    }

    pub fn inject_ctrl_cmd(&mut self, cmd_name: &str, cmd_value: &str) -> Result<()> {
        match cmd_name {
            "display_pretty_sql" => self.display_pretty_sql = cmd_value.parse()?,
            "prompt" => self.prompt = cmd_value.to_string(),
            "progress_color" => self.progress_color = cmd_value.to_string(),
            "show_progress" => self.show_progress = cmd_value.parse()?,
            "show_stats" => self.show_stats = cmd_value.parse()?,
            "output_format" => {
                self.output_format = match cmd_value.to_ascii_lowercase().as_str() {
                    "table" => OutputFormat::Table,
                    "csv" => OutputFormat::CSV,
                    "tsv" => OutputFormat::TSV,
                    "null" => OutputFormat::Null,
                    _ => return Err(anyhow!("Unknown output format: {}", cmd_value)),
                }
            }
            "quote_style" => {
                self.quote_style = match cmd_value.to_ascii_lowercase().as_str() {
                    "necessary" => OutputQuoteStyle::Necessary,
                    "always" => OutputQuoteStyle::Always,
                    "never" => OutputQuoteStyle::Never,
                    "nonnumeric" => OutputQuoteStyle::NonNumeric,
                    _ => return Err(anyhow!("Unknown quote style: {}", cmd_value)),
                }
            }
            "expand" => self.expand = cmd_value.into(),
            "time" => self.time = Some(cmd_value.try_into()?),
            "multi_line" => self.multi_line = cmd_value.parse()?,
            "max_display_rows" => self.max_display_rows = cmd_value.parse()?,
            "max_width" => self.max_width = cmd_value.parse()?,
            "max_col_width" => self.max_col_width = cmd_value.parse()?,
            "replace_newline" => self.replace_newline = cmd_value.parse()?,
            _ => return Err(anyhow!("Unknown command: {}", cmd_name)),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub database: Option<String>,
    pub args: BTreeMap<String, String>,
}

impl Config {
    pub fn load() -> Self {
        let paths = [
            format!(
                "{}/.bendsql/config.toml",
                std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
            ),
            format!(
                "{}/.config/bendsql/config.toml",
                std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
            ),
        ];
        let path = paths.iter().find(|p| Path::new(p).exists());
        match path {
            Some(path) => Self::load_from_file(path),
            None => Self::default(),
        }
    }

    fn load_from_file(path: &str) -> Self {
        match toml::from_str(&std::fs::read_to_string(path).unwrap()) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("failed to load config file {}: {}, using defaults", path, e);
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
            prompt: "{user}@{warehouse}/{database}> ".to_string(),
            no_auto_complete: false,
            output_format: OutputFormat::Table,
            quote_style: OutputQuoteStyle::Necessary,
            expand: ExpandMode::Auto,
            show_progress: false,
            max_display_rows: 40,
            max_col_width: 1024 * 1024,
            max_width: 1024 * 1024,
            show_stats: false,
            time: None,
            multi_line: true,
            replace_newline: true,
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: Some(8000),
            user: "root".to_string(),
            database: None,
            args: BTreeMap::new(),
        }
    }
}
