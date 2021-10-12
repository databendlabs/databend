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
use std::os::unix::prelude::FromRawFd;
use std::os::unix::prelude::IntoRawFd;
use std::path::Path;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time;

use databend_query::configs::Config as QueryConfig;
use libc::pid_t;
use log::info;
use metasrv::configs::Config as MetaConfig;
use nix::unistd::Pid;

use crate::cmds::Config;
use crate::error::CliError;
use crate::error::Result;
use std::collections::HashMap;
use serde::Serialize;
use serde_json::Map;
use colored::Colorize;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct Status {
    pub path: String,
    pub version: String,
    pub local_configs: HashMap<String, String>,
    pub local_config_dir: String,
    pub current_profile: Option<String>,
}

/// TODO(zhihanz) extension configurations
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct LocalConfig {
    pub query_configs: Vec<LocalQueryConfig>,
    pub store_configs: Option<LocalMetaConfig>,
    pub meta_configs: Option<LocalMetaConfig>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalQueryConfig {
    pub config: QueryConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
    pub log_dir: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct LocalMetaConfig {
    pub config: MetaConfig,
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
                    Ok(_) => Ok(()),
                    Err(e) => Err(CliError::from(e)),
                }
            }
            None => Ok(()),
        }
    }
    fn start(&mut self) -> Result<()> {
        if self.get_pid().is_some() {
            return Err(CliError::Unknown(format!(
                "current instance in path {} already started",
                self.get_path().expect("cannot retrieve executable path")
            )));
        }
        let mut cmd = self.generate_command().expect("cannot parse command");
        let child = cmd.spawn().expect("cannot execute command");
        self.set_pid(child.id() as pid_t);
        match self.verify() {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
    fn get_pid(&self) -> Option<pid_t>;
    fn verify(&self) -> Result<()>;
    fn get_path(&self) -> Option<String>;
    fn generate_command(&mut self) -> Result<Command>;
    fn set_pid(&mut self, id: pid_t);
}

impl LocalRuntime for LocalMetaConfig {
    const RETRIES: u16 = 5;

    fn get_pid(&self) -> Option<pid_t> {
        self.pid
    }

    // will check health for endpoint through http request
    fn verify(&self) -> Result<()> {
        let (cli, url) = self.get_health_probe();
        for _ in 0..LocalMetaConfig::RETRIES {
            let resp = cli.get(url.as_str()).send();
            if resp.is_err() || !resp.unwrap().status().is_success() {
                sleep(time::Duration::from_secs(1));
            } else {
                return Ok(());
            }
        }
        return Err(CliError::Unknown(format!(
            "cannot fetch healthness status for store instance: {}",
            url
        )));
    }

    fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    // bootstrap store command
    fn generate_command(&mut self) -> Result<Command> {
        let conf = self.config.clone();
        if self.path.is_none() {
            return Err(CliError::Unknown(
                "cannot retrieve store binary execution path"
                    .parse()
                    .unwrap(),
            ));
        }
        let mut command = Command::new(self.path.clone().unwrap());
        let log_dir = format!(
            "{}/_local_logs",
            self.log_dir
                .as_ref()
                .expect("cannot find log dir for store")
        );

        if !Path::new(log_dir.as_str()).exists() {
            std::fs::create_dir(Path::new(log_dir.as_str()))
                .unwrap_or_else(|_| panic!("cannot create directory {}", log_dir));
        }

        let out_file = File::create(format!("{}/std_out.log", log_dir).as_str())
            .expect("couldn't create stdout file");
        let err_file = File::create(format!("{}/std_err.log", log_dir).as_str())
            .expect("couldn't create stdout file");
        // configure runtime by process local env settings
        // TODO(zhihanz): configure on other needed env variables for raft metastore
        command
            .env(metasrv::configs::config::METASRV_LOG_LEVEL, conf.log_level)
            .env(metasrv::configs::config::METASRV_LOG_DIR, conf.log_dir)
            .env(
                metasrv::configs::config::METASRV_FLIGHT_API_ADDRESS,
                conf.flight_api_address,
            )
            .env(
                metasrv::configs::config::ADMIN_API_ADDRESS,
                conf.admin_api_address,
            )
            .env(
                metasrv::configs::config::METASRV_METRIC_API_ADDRESS,
                conf.metric_api_address,
            )
            .env(
                metasrv::configs::config::FLIGHT_TLS_SERVER_CERT,
                conf.flight_tls_server_cert,
            )
            .env(
                metasrv::configs::config::FLIGHT_TLS_SERVER_KEY,
                conf.flight_tls_server_key,
            )
            .env(
                metasrv::configs::config::ADMIN_TLS_SERVER_CERT,
                conf.admin_tls_server_cert,
            )
            .env(
                metasrv::configs::config::ADMIN_TLS_SERVER_KEY,
                conf.admin_tls_server_key,
            )
            .env(
                common_meta_raft_store::config::KVSRV_SINGLE,
                conf.raft_config.single.to_string(),
            )
            .env(
                common_meta_raft_store::config::KVSRV_RAFT_DIR,
                conf.raft_config.raft_dir,
            )
            .env(
                common_meta_raft_store::config::KVSRV_API_PORT,
                conf.raft_config.raft_api_port.to_string(),
            )
            .env(
                common_meta_raft_store::config::KVSRV_API_HOST,
                conf.raft_config.raft_api_host,
            )
            .stdout(unsafe { Stdio::from_raw_fd(out_file.into_raw_fd()) })
            .stderr(unsafe { Stdio::from_raw_fd(err_file.into_raw_fd()) });
        // logging debug
        info!("executing command {:?}", command);
        Ok(command)
    }

    fn set_pid(&mut self, id: pid_t) {
        self.pid = Some(id)
    }
}

impl LocalRuntime for LocalQueryConfig {
    const RETRIES: u16 = 5;

    fn get_pid(&self) -> Option<pid_t> {
        self.pid
    }

    fn verify(&self) -> Result<()> {
        let (cli, url) = self.get_health_probe();
        for _ in 0..LocalQueryConfig::RETRIES {
            let resp = cli.get(url.as_str()).send();
            if resp.is_err() || !resp.unwrap().status().is_success() {
                sleep(time::Duration::from_secs(1));
            } else {
                return Ok(());
            }
        }
        return Err(CliError::Unknown(format!(
            "cannot fetch healthness status for query instance: {}",
            url
        )));
    }

    fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    // command build by
    fn generate_command(&mut self) -> Result<Command> {
        let mut conf = self.config.clone();
        if !conf.config_file.is_empty() {
            // logging debug
            conf = databend_query::configs::Config::load_from_toml(conf.config_file.as_str())
                .expect("query instance configuration cannot load from toml");
        }
        conf = databend_query::configs::Config::load_from_env(&conf)
            .expect("cannot parse env variable for query configuration");
        if self.path.is_none() {
            return Err(CliError::Unknown(
                "cannot retrieve query instance execution path"
                    .parse()
                    .unwrap(),
            ));
        }
        self.config = conf.clone(); // update configurations
        let mut command = Command::new(self.path.clone().unwrap());
        let log_dir = self.log_dir.as_ref().unwrap();
        let out_file = File::create(format!("{}/std_out.log", log_dir).as_str())
            .expect("couldn't create stdout file");
        let err_file = File::create(format!("{}/std_err.log", log_dir).as_str())
            .expect("couldn't create stdout file");
        // configure runtime by process local env settings
        command
            .env(
                databend_query::configs::config_log::LOG_LEVEL,
                conf.log.log_level,
            )
            .env(
                databend_query::configs::config_log::LOG_DIR,
                conf.log.log_dir,
            )
            .env(
                databend_query::configs::config_query::QUERY_NAMESPACE,
                conf.query.namespace,
            )
            .env(
                databend_query::configs::config_query::QUERY_TENANT,
                conf.query.tenant,
            )
            .env(
                databend_query::configs::config_query::QUERY_NUM_CPUS,
                conf.query.num_cpus.to_string(),
            )
            .env(
                databend_query::configs::config_query::QUERY_CLICKHOUSE_HANDLER_HOST,
                conf.query.clickhouse_handler_host,
            )
            .env(
                databend_query::configs::config_query::QUERY_CLICKHOUSE_HANDLER_PORT,
                conf.query.clickhouse_handler_port.to_string(),
            )
            .env(
                databend_query::configs::config_query::QUERY_MYSQL_HANDLER_HOST,
                conf.query.mysql_handler_host,
            )
            .env(
                databend_query::configs::config_query::QUERY_MYSQL_HANDLER_PORT,
                conf.query.mysql_handler_port.to_string(),
            )
            .env(
                databend_query::configs::config_query::QUERY_FLIGHT_API_ADDRESS,
                conf.query.flight_api_address,
            )
            .env(
                databend_query::configs::config_query::QUERY_HTTP_API_ADDRESS,
                conf.query.http_api_address,
            )
            .env(
                databend_query::configs::config_query::QUERY_METRICS_API_ADDRESS,
                conf.query.metric_api_address,
            )
            .stdout(unsafe { Stdio::from_raw_fd(out_file.into_raw_fd()) })
            .stderr(unsafe { Stdio::from_raw_fd(err_file.into_raw_fd()) });
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
    pub fn get_health_probe(&self) -> (reqwest::blocking::Client, String) {
        let client = reqwest::blocking::Client::builder()
            .build()
            .expect("Cannot build health probe for health check");

        let url = {
            if !self.config.tls_rpc_server_enabled() {
                format!("http://{}/v1/health", self.config.query.http_api_address)
            } else {
                todo!()
            }
        };
        (client, url)
    }
}

impl LocalMetaConfig {
    // retrieve the configured url for health check
    // TODO(zhihanz): http TLS endpoint
    pub fn get_health_probe(&self) -> (reqwest::blocking::Client, String) {
        let client = reqwest::blocking::Client::builder()
            .build()
            .expect("Cannot build health probe for health check");

        let url = {
            if self.config.admin_tls_server_key.is_empty()
                || self.config.admin_tls_server_cert.is_empty()
            {
                format!("http://{}/v1/health", self.config.admin_api_address)
            } else {
                todo!()
            }
        };
        (client, url)
    }
}

impl Status {
    pub fn read(conf: Config) -> Result<Self> {
        let status_path = format!("{}/.status.json", conf.databend_dir);
        log::info!("{}", status_path.as_str());
        let local_config_dir = format!("{}/configs/local", conf.databend_dir);
        std::fs::create_dir_all(local_config_dir.as_str()).expect("cannot create dir to store local profile");
        if !Path::new(status_path.as_str()).exists() {
            // Create.
            let file = File::create(status_path.as_str())?;
            let status = Status {
                path: status_path.clone(),
                version: "".to_string(),
                local_configs: HashMap::new(),
                local_config_dir,
                current_profile: None,
            };
            serde_json::to_writer(&file, &status)?;
        }

        let file = File::open(status_path)?;
        let reader = BufReader::new(file);
        let status: Status = serde_json::from_reader(reader)?;
        Ok(status)
    }

    pub fn save_local_config<T>(status: &mut Status, config_type: String, file_name: String, data: &T) -> Result<()>
        where
            T: ?Sized + Serialize,
    {
        if config_type.as_str() != "meta" && config_type.as_str() != "query" {
            return Err(CliError::Unknown("Unsupported config type for local storage".parse().unwrap()))
        }
        let file_location = format!("{}/{}", status.local_config_dir, file_name);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_location.clone())?;
        serde_json::to_writer(&file, data).expect(&*format!("cannot write to file {}", file_location.clone()));
        if status.local_configs.get(&*config_type).is_none() {
            status.local_configs.insert(config_type, file_location);
        } else {
            let mut current_configs :Vec<&str> = status.local_configs.get(&*config_type).unwrap().split(',').collect();
            current_configs.push(&*file_location);
            status.local_configs.insert(config_type, current_configs.join(","));
        }
        status.write()?;
        Ok(())
    }

    pub fn delete_local_config(status: &mut Status, config_type: String, file_name: String) -> Result<()>
    {
        if config_type.as_str() != "meta" && config_type.as_str() != "query" {
            return Err(CliError::Unknown("Unsupported config type for local storage".parse().unwrap()))
        }
        if status.local_configs.get(config_type.as_str()).is_none() {
            return Ok(())
        }
        std::fs::remove_file(file_name.clone()).expect("cannot delete config");
        let mut vec = status.local_configs.get(config_type.as_str()).unwrap().split(',').collect::<Vec<&str>>();
        vec.retain(|s| s.to_string() != file_name);
        status.local_configs.insert(config_type, vec.join(","));
        status.write()?;
        Ok(())
    }

    pub fn get_local_meta_config(&self) -> Option<(String, LocalMetaConfig)> {
        if self.local_configs.get("meta").is_none() || self.local_configs.get("meta").unwrap().is_empty() {
            return None
        }
        let meta_file = self.local_configs.get("meta").unwrap();
        let splited  = meta_file.as_str().split(",").collect::<Vec<&str>>();
        let meta_file = splited.get(0).unwrap();
        if !Path::new(meta_file.clone()).exists() {
            return None
        }
        let file = File::open(meta_file.clone()).expect(&*format!("cannot read from {}", meta_file.clone()));
        let reader = BufReader::new(file);
        return Some((meta_file.clone().to_string(),serde_json::from_reader(reader).expect(&*format!("cannot read from {}", meta_file))))
    }

    pub fn has_local_configs(&self) -> bool {
        for (k, v) in &self.local_configs {
            let mut v = v.clone().retain(|c| c != ',');
            if !v.is_empty() {
                return true
            }
        }
        return false
    }

    pub fn get_local_query_configs(&self) -> Vec<(String, LocalQueryConfig)> {
        if self.local_configs.get("query").is_none() {
            return Vec::new()
        }
        let query_files = self.local_configs.get("query").unwrap().split(",").collect::<Vec<&str>>();
        let mut ret = Vec::new();
        for file_name in query_files {
            if !Path::new(file_name.clone()).exists() {
                continue
            }
            let file = File::open(file_name.clone()).unwrap();
            let reader = BufReader::new(file);
            ret.push((file_name.to_string(), serde_json::from_reader(reader).expect(&*format!("cannot read from {}", file_name))));
        }
        return ret
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
        format!(
            "0.0.0.0:{}",
            portpicker::pick_unused_port().expect("cannot find a non-occupied port")
        )
    }
}
