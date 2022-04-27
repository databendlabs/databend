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

use std::str::FromStr;

use clap::Args;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ThriftProtocol {
    Binary,
    // Compact,
}

impl FromStr for ThriftProtocol {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<ThriftProtocol, &'static str> {
        let s = s.to_lowercase();
        match s.as_str() {
            "binary" => Ok(ThriftProtocol::Binary),
            _ => Err("invalid thrift protocol spec"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct HiveCatalogConfig {
    #[clap(long = "hive-meta-store-address", default_value = "127.0.0.1:9083")]
    pub meta_store_address: String,
    #[clap(long = "hive-thrift-protocol")]
    pub protocol: ThriftProtocol,
}

impl Default for HiveCatalogConfig {
    fn default() -> Self {
        Self {
            meta_store_address: "127.0.0.1:9083".to_string(),
            protocol: ThriftProtocol::Binary,
        }
    }
}
