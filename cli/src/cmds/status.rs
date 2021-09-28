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
use std::process::{Command, Stdio};
use std::time;
use crate::cmds::cluster::cluster::ClusterProfile;
use std::os::unix::prelude::{AsRawFd, FromRawFd, IntoRawFd};
use reqwest::Client;
use std::thread::sleep;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct Status {
    pub path: String,
    pub version: String,
    pub local_configs: LocalConfig,
    pub current_profile: Option<String>,
}

/// TODO(zhihanz) extension configurations
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalConfig {
    pub query_configs: Vec<LocalQueryConfig>,
    pub store_configs: Option<LocalStoreConfig>,
    pub meta_configs: Option<LocalStoreConfig>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalQueryConfig {
    pub config: QueryConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalStoreConfig {
    pub config: StoreConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
    pub log_dir: Option<String>,
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
        Ok(())
    }
    fn get_pid(&self) -> Option<pid_t>;
    fn verify(&self)  -> Result<()> ;
    fn get_path(&self) -> Option<String>;
    fn generate_command(&mut self) -> Result<Command>;
    fn set_pid(&mut self, id: pid_t);
}

impl LocalRuntime for LocalStoreConfig {
    const RETRIES: u16 = 5;

    fn get_pid(&self) -> Option<pid_t> {
        self.pid
    }

    // will check health for endpoint through http request
    fn verify(&self) -> Result<()> {
        let (cli, url) = self.get_health_probe();
        for _ in 0..LocalStoreConfig::RETRIES {
            let resp = cli.get(url.as_str()).send();
            if !resp.is_ok() || !resp.unwrap().status().is_success() {
                sleep(time::Duration::from_secs(1));
            } else {
                return Ok(())
            }
        }
        return Err(CliError::Unknown(format!("cannot fetch healthness status for store instance: {}", url)))
    }

    fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    // bootstrap store command
    fn generate_command(&mut self) -> Result<Command> {
        let conf = self.config.clone();
        if self.path.is_none() {
            return Err(CliError::Unknown("cannot retrieve store binary execution path".parse().unwrap()));
        }
        let mut command = Command::new(self.path.clone().unwrap());
        let log_dir = format!("{}/_local_logs", self.log_dir.as_ref().expect("cannot find log dir for store"));

        if !Path::new(log_dir.as_str()).exists() {
            std::fs::create_dir(Path::new(log_dir.as_str())).expect(format!("cannot create directory {}", log_dir).as_str());
        }

        let out_file = File::create(format!("{}/std_out.log", log_dir).as_str()).expect("couldn't create stdout file");
        let err_file = File::create(format!("{}/std_err.log", log_dir).as_str()).expect("couldn't create stdout file");
        // configure runtime by process local env settings
        // TODO(zhihanz): configure on other needed env variables for raft metastore
        command.env(databend_dfs::configs::config::STORE_LOG_LEVEL, conf.log_level)
            .env(databend_dfs::configs::config::STORE_LOG_DIR, conf.log_dir)
            .env(databend_dfs::configs::config::STORE_FLIGHT_API_ADDRESS, conf.flight_api_address)
            .env(databend_dfs::configs::config::STORE_LOCAL_FS_DIR, conf.local_fs_dir)
            .env(databend_dfs::configs::config::STORE_HTTP_API_ADDRESS, conf.http_api_address)
            .env(databend_dfs::configs::config::STORE_METRIC_API_ADDRESS, conf.metric_api_address)
            .env(databend_dfs::configs::config::STORE_RPC_TLS_SERVER_CERT, conf.rpc_tls_server_cert)
            .env(databend_dfs::configs::config::STORE_RPC_TLS_SERVER_KEY, conf.rpc_tls_server_key)
            .env(databend_dfs::configs::config::STORE_TLS_SERVER_CERT, conf.tls_server_cert)
            .env(databend_dfs::configs::config::STORE_TLS_SERVER_KEY, conf.tls_server_key)
            .env(common_raft_store::config::KVSRV_SINGLE, conf.meta_config.single.to_string())
            .stdout(unsafe { Stdio::from_raw_fd(out_file.into_raw_fd()) })
            .stderr(unsafe { Stdio::from_raw_fd(err_file.into_raw_fd()) });
        // logging debug
        info!("executing command {:?}", command);
        match self.verify() {
            Ok(_) => {
                Ok(command)
            }
            Err(e) => {
                return Err(e)
            }
        }
    }

    fn set_pid(&mut self, id: pid_t) {
        self.pid = Some(id)
    }
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

impl LocalStoreConfig {
    // retrieve the configured url for health check
    // TODO(zhihanz): http TLS endpoint
    pub fn get_health_probe(&self) -> (reqwest::blocking::Client, String) {
        let client = reqwest::blocking::Client::builder()
            .build()
            .expect("Cannot build health probe for health check");

        let url = {
            if self.config.tls_server_key.is_empty() || self.config.tls_server_cert.is_empty() {
                format!("http://{}/v1/health", self.config.http_api_address)
            } else {
                format!("")
            }
        };
        return (client, url);
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
                current_profile: None,
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
    pub fn find_unused_local_port() -> String {
        format!("0.0.0.0:{}", portpicker::pick_unused_port().expect("cannot find a non-occupied port"))
    }
}
