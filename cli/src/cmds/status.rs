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
use nix::unistd::Pid;
use log::{info, warn};

use databend_dfs::configs::Config as StoreConfig;
use databend_query::configs::{Config as QueryConfig};

use crate::cmds::Config;
use crate::error::{Result, CliError};
use std::os::unix::raw::pid_t;
use std::process::Command;
use std::thread::sleep;
use std::time;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct Status {
    pub path: String,
    pub version: String,
    pub local_configs: LocalConfig,
}

/// TODO(zhihanz) extension configurations
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalConfig {
    pub query_configs: Vec<LocalQueryConfig>,
    pub store_configs: Option<LocalStoreConfig>,
    pub meta_configs: Option<LocalStoreConfig>,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalQueryConfig {
    pub config: QueryConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalStoreConfig {
    pub config: StoreConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
}

impl LocalConfig {
    pub fn empty() -> Self {
        LocalConfig {
            query_configs: vec![],
            store_configs: None,
            meta_configs: None,
        }
    }
}

pub trait LocalRuntime {
    const RETRIES: u16;
    fn kill(&self) -> Result<()> {
        let pid = self.get_pid();
        match pid {
            Some(id) => {
                match nix::sys::signal::kill(Pid::from_raw(id), Some(nix::sys::signal::SIGINT)) {
                    Ok(_) => {
                        Ok(())
                    }
                    Err(e) => {
                        Err(CliError::from(e))
                    }
                }
            }
            None => {
                Ok(())
            }
        }
    }
    fn start(&mut self) -> Result<()> {
        if self.get_pid().is_some() {
            return Err(CliError::Unknown(format!("current instance in path {} already started", self.get_path().expect("cannot retrieve executable path"))));
        }
        let mut cmd = self.generate_command().expect("cannot parse command");
        let child = cmd.spawn().expect("cannot execute command");
        self.set_pid(child.id() as pid_t);
        self.verify()
    }
    fn get_pid(&self) -> Option<pid_t>;
    fn verify(&self)  -> Result<()> ;
    fn get_path(&self) -> Option<String>;
    fn generate_command(&mut self) -> Result<Command>;
    fn set_pid(&mut self, id: pid_t);
}

impl LocalRuntime for LocalQueryConfig{
    const RETRIES: u16 = 5;

    fn get_pid(&self) -> Option<pid_t> {
        self.pid
    }

    fn verify(&self) -> Result<()> {
        todo!()
    }

    fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    // command build by
    fn generate_command(&mut self) -> Result<Command> {
        let mut conf = self.config.clone();
        if !conf.config_file.is_empty() {
            // logging debug
            conf = databend_query::configs::Config::load_from_toml(conf.config_file.as_str()).expect("query instance configuration cannot load from toml");
        }
        conf = databend_query::configs::Config::load_from_env(&conf).expect("cannot parse env variable for query configuration");
        if self.path.is_none() {
            return Err(CliError::Unknown("cannot retrieve query instance execution path".parse().unwrap()));
        }
        self.config = conf.clone(); // update configurations
        let mut command = Command::new(self.path.clone().unwrap());
        // configure runtime by process local env settings
        command.env(databend_query::configs::config::LOG_LEVEL, conf.log.log_level);
        // logging debug
        info!("executing command {:?}", command);
        Ok(command)
    }

    fn set_pid(&mut self, id: pid_t) {
        self.pid = Some(id)
    }
}

impl LocalQueryConfig {
    // retrieve the configured url for health check
    pub fn get_health_endpoint(&self) -> Option<String> {
        todo!()
    }
}

impl Status {
    pub fn read(conf: Config) -> Result<Self> {
        let status_path = format!("{}/.status.json", conf.databend_dir);
        log::info!("{}", status_path.as_str());
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
