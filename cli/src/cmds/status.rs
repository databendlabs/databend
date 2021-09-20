// Copyright 2020 Datafuse Labs.
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

use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::Path;
use databend_query::configs::Config as QueryConfig;
use databend_store::configs::Config as StoreConfig;
use crate::cmds::Config;
use crate::error::Result;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct Status {
    path: String,
    pub version: String,
    pub local_configs: LocalConfig,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalConfig {
    pub query_configs: Vec<LocalQueryConfig>,
    pub store_configs: Option<LocalStoreConfig>,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalQueryConfig {
    pub config: QueryConfig,
    pub pid: String,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalStoreConfig {
    pub config: StoreConfig,
    pub pid: String,
}

impl LocalConfig {
    pub fn empty() -> Self {
        return LocalConfig{
            query_configs: vec![],
            store_configs: None
        }
    }
}



impl Status {
    pub fn read(conf: Config) -> Result<Self> {
        let status_path = format!("{}/.status.json", conf.databend_dir);

        if !Path::new(status_path.as_str()).exists() {
            // Create.
            let file = File::create(status_path.as_str())?;
            let status = Status {
                path: status_path.clone(),
                version: "".to_string(),
                local_configs: LocalConfig::empty(),
            };
            serde_json::to_writer(&file, &status)?;
        }

        let file = File::open(status_path)?;
        let reader = BufReader::new(file);
        let status: Status = serde_json::from_reader(reader)?;
        Ok(status)
    }

    pub fn write(&self) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path.clone())?;
        serde_json::to_writer(&file, self)?;
        Ok(())
    }
}
