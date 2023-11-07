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

use std::fmt::Display;
use std::fmt::Formatter;

/// Config for logging.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct Config {
    pub file: FileConfig,
    pub stderr: StderrConfig,
    pub query: QueryLogConfig,
    pub tracing: TracingConfig,
}

impl Config {
    /// new_testing is used for create a Config suitable for testing.
    pub fn new_testing() -> Self {
        Self {
            file: FileConfig {
                on: true,
                level: "DEBUG".to_string(),
                dir: "./.databend/logs".to_string(),
                format: "text".to_string(),
            },
            stderr: StderrConfig {
                on: true,
                level: "WARN".to_string(),
                format: "text".to_string(),
            },
            query: QueryLogConfig {
                on: true,
                dir: "./.databend/logs/query-details".to_string(),
            },
            tracing: TracingConfig {
                on: false,
                capture_log_level: "TRACE".to_string(),
                otlp_endpoint: "http://127.0.0.1:4317".to_string(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct FileConfig {
    pub on: bool,
    pub level: String,
    pub dir: String,
    pub format: String,
}

impl Display for FileConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, level={}, dir={}, format={}",
            self.on, self.level, self.dir, self.format
        )
    }
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            on: true,
            level: "INFO".to_string(),
            dir: "./.databend/logs".to_string(),
            format: "json".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct StderrConfig {
    pub on: bool,
    pub level: String,
    pub format: String,
}

impl Display for StderrConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}{}, level={}, format={}",
            self.on,
            if !self.on {
                "(To enable: LOG_STDERR_ON=true or RUST_LOG=info)"
            } else {
                ""
            },
            self.level,
            self.format,
        )
    }
}

impl Default for StderrConfig {
    fn default() -> Self {
        Self {
            on: false,
            level: "INFO".to_string(),
            format: "text".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct QueryLogConfig {
    pub on: bool,
    pub dir: String,
}

impl Display for QueryLogConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "enabled={}, dir={}", self.on, self.dir)
    }
}

impl Default for QueryLogConfig {
    fn default() -> Self {
        Self {
            on: true,
            dir: "./.databend/logs/query-details".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct TracingConfig {
    pub on: bool,
    pub capture_log_level: String,
    pub otlp_endpoint: String,
}

impl Display for TracingConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enabled={}, capture_log_level={}, otlp_endpoint={}",
            self.on, self.capture_log_level, self.otlp_endpoint
        )
    }
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            on: false,
            capture_log_level: "INFO".to_string(),
            otlp_endpoint: "http://localhost:4317".to_string(),
        }
    }
}
