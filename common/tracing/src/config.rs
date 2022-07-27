// Copyright 2022 Datafuse Labs.
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

/// Config for tracing.
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize)]
pub struct Config {
    pub file: FileConfig,
    pub stderr: StderrConfig,
}

impl Config {
    /// new_testing is used for create a Config suitable for testing.
    pub fn new_testing() -> Self {
        Self {
            file: FileConfig {
                on: true,
                level: "DEBUG".to_string(),
                dir: "./.databend/logs".to_string(),
            },
            stderr: StderrConfig {
                on: true,
                level: "DEBUG".to_string(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct FileConfig {
    pub on: bool,
    pub level: String,
    pub dir: String,
    // TODO: Add format support in the future, before that we use `json`
    // pub format: String,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            on: true,
            level: "INFO".to_string(),
            dir: "./.databend/logs".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct StderrConfig {
    pub on: bool,
    pub level: String,
    // TODO: Add format support in the future, before that we use `text`
    // pub format: String,
}

impl Default for StderrConfig {
    fn default() -> Self {
        Self {
            on: false,
            level: "INFO".to_string(),
        }
    }
}
