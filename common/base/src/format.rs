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

use std::path::Path;

use common_exception::ErrorCode;
use common_exception::Result;
use serde::de;
use serde::ser;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Format {
    Toml,
    Json,
    Yaml,
}

impl Default for Format {
    fn default() -> Self {
        Format::Toml
    }
}

impl Format {
    /// Obtain the format from the file path using the file extension
    /// Format is case sensitive
    pub fn from_path<T: AsRef<Path>>(path: T) -> Result<Self> {
        match path.as_ref().extension().and_then(|ext| ext.to_str()) {
            Some("toml") => Ok(Format::Toml),
            Some("yaml") | Some("yml") => Ok(Format::Yaml),
            Some("json") => Ok(Format::Json),
            other => Err(ErrorCode::BadArguments(format!(
                "Unknown format type {:?}",
                other
            ))),
        }
    }

    pub fn load_config<T>(&self, content: &str) -> Result<T>
    where T: de::DeserializeOwned {
        match self {
            Format::Toml => {
                toml::from_str(content).map_err(|e| ErrorCode::BadArguments(format!("{:?}", e)))
            }
            Format::Yaml => serde_yaml::from_str(content)
                .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e))),
            Format::Json => serde_json::from_str(content)
                .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e))),
        }
    }

    pub fn serialize_config<T>(&self, value: &T) -> Result<String>
    where T: ser::Serialize {
        match self {
            Format::Toml => {
                toml::to_string(value).map_err(|e| ErrorCode::BadArguments(format!("{:?}", e)))
            }
            Format::Yaml => serde_yaml::to_string(value)
                .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e))),
            Format::Json => serde_json::to_string(value)
                .map_err(|e| ErrorCode::BadArguments(format!("{:?}", e))),
        }
    }
}
