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

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::net::SocketAddr;
use std::os::unix::prelude::FromRawFd;
use std::os::unix::prelude::IntoRawFd;
use std::path::Path;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time;

use async_trait::async_trait;
use common_base::tokio::time::Duration;
use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use libc::pid_t;
use log::info;
use nix::unistd::Pid;
use reqwest::Client;
use serde::Deserialize;
use serde::Serialize;
use sysinfo::System;
use sysinfo::SystemExt;

use crate::cmds::config::CustomMirror;
use crate::cmds::Config;
use crate::error::CliError;
use crate::error::Result;

const RETRIES: u16 = 30;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Status {
    pub path: String,
    pub version: String,
    pub local_configs: HashMap<String, String>,
    pub local_config_dir: String,
    pub current_profile: Option<String>,
    pub mirrors: Option<CustomMirror>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct LocalQueryConfig {
    pub config: QueryConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
    pub log_dir: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct LocalMetaConfig {
    pub config: MetaConfig,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
    pub log_dir: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct LocalDashboardConfig {
    pub listen_addr: Option<String>,
    pub http_api: Option<String>,
    pub pid: Option<pid_t>,
    pub path: Option<String>, // download location
    pub log_dir: Option<String>,
}

async fn check_health(
    cli: reqwest::Client,
    url: String,
    duration: std::time::Duration,
) -> Result<()> {
    let resp = cli.get(url.as_str()).send().await;
    if resp.is_err() || !resp.unwrap().status().is_success() {
        async_std::task::sleep(duration).await;
        return Err(CliError::Unknown(format!(
            "cannot connect to healthiness probe: {}",
            url
        )));
    } else {
        Ok(())
    }
}

#[async_trait]
pub trait LocalRuntime: Send + Sync {
    async fn kill(&self) -> Result<()> {
        let pid = self.get_pid();
        match pid {
            Some(id) => {
                for _ in 0..10 {
                    if !self.is_clean() {
                        if nix::sys::signal::kill(Pid::from_raw(id), Some(nix::sys::signal::SIGINT))
                            .is_ok()
                            && self.is_clean()
                        {
                            return Ok(());
                        }
                        if nix::sys::signal::kill(
                            Pid::from_raw(id),
                            Some(nix::sys::signal::SIGTERM),
                        )
                        .is_ok()
                            && self.is_clean()
                        {
                            return Ok(());
                        }
                        sleep(time::Duration::from_secs(1));
                    } else {
                        return Ok(());
                    }
                }

                if self.is_clean() {
                    return Ok(());
                } else {
                    if nix::sys::signal::kill(Pid::from_raw(id), Some(nix::sys::signal::SIGKILL))
                        .is_err()
                    {}
                    return Ok(());
                }
            }
            None => Ok(()),
        }
    }
    async fn start(&mut self) -> Result<()> {
        if self.get_pid().is_some() {
            return Err(CliError::Unknown(format!(
                "current instance in path {} already started",
                self.get_path().expect("cannot retrieve executable path")
            )));
        }
        let mut cmd = self.generate_command().expect("cannot parse command");
        let child = cmd.spawn().expect("cannot execute command");
        self.set_pid(child.id() as pid_t);
        match self.verify(None, None).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
    fn get_pid(&self) -> Option<pid_t>;
    async fn verify(
        &self,
        retries: Option<u32>,
        duration: Option<std::time::Duration>,
    ) -> Result<()> {
        let (retries, duration) = (
            retries.unwrap_or(RETRIES as u32),
            duration.unwrap_or_else(|| std::time::Duration::from_secs(1)),
        );
        let (cli, url) = self.get_health_probe();
        let result = tryhard::retry_fn(move || check_health(cli.clone(), url.clone(), duration))
            .retries(retries)
            .await;
        return result;
    }
    fn get_path(&self) -> Option<String>;
    fn generate_command(&mut self) -> Result<Command>;
    fn set_pid(&mut self, id: pid_t);
    fn is_clean(&self) -> bool; // return true if runtime resources are cleaned
    fn get_health_probe(&self) -> (reqwest::Client, String);
}

#[async_trait]
impl LocalRuntime for LocalDashboardConfig {
    fn get_pid(&self) -> Option<pid_t> {
        self.pid
    }

    fn get_path(&self) -> Option<String> {
        self.path.clone()
    }

    fn generate_command(&mut self) -> Result<Command> {
        if self.path.is_none() {
            return Err(CliError::Unknown(
                "cannot retrieve playground binary execution path"
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
            .expect("couldn't create stderr file");
        // configure runtime by process local env settings
        command
            .arg("--listen-addr")
            .arg(
                self.listen_addr
                    .as_ref()
                    .expect("did not configured listen address for playground"),
            )
            .arg("--bend-http-api")
            .arg(
                self.http_api
                    .as_ref()
                    .expect("did not configured http handler address for playground"),
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

    fn is_clean(&self) -> bool {
        if self.pid.is_none() {
            return true;
        }
        let s = System::new_all();
        let pid = self.pid.unwrap();
        return s.process(pid).is_none();
    }

    async fn verify(&self, _retries: Option<u32>, _duration: Option<Duration>) -> Result<()> {
        Ok(())
    }

    fn get_health_probe(&self) -> (Client, String) {
        todo!()
    }
}

#[async_trait]
impl LocalRuntime for LocalMetaConfig {
    fn get_pid(&self) -> Option<pid_t> {
        self.pid
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
            .env(
                databend_meta::configs::config::METASRV_LOG_LEVEL,
                conf.log_level,
            )
            .env(
                databend_meta::configs::config::METASRV_LOG_DIR,
                conf.log_dir,
            )
            .env(
                databend_meta::configs::config::METASRV_FLIGHT_API_ADDRESS,
                conf.flight_api_address,
            )
            .env(
                databend_meta::configs::config::ADMIN_API_ADDRESS,
                conf.admin_api_address,
            )
            .env(
                databend_meta::configs::config::METASRV_METRIC_API_ADDRESS,
                conf.metric_api_address,
            )
            .env(
                databend_meta::configs::config::FLIGHT_TLS_SERVER_CERT,
                conf.flight_tls_server_cert,
            )
            .env(
                databend_meta::configs::config::FLIGHT_TLS_SERVER_KEY,
                conf.flight_tls_server_key,
            )
            .env(
                databend_meta::configs::config::ADMIN_TLS_SERVER_CERT,
                conf.admin_tls_server_cert,
            )
            .env(
                databend_meta::configs::config::ADMIN_TLS_SERVER_KEY,
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

    fn is_clean(&self) -> bool {
        if self.pid.is_none() {
            return true;
        }
        let s = System::new_all();
        let pid = self.pid.unwrap();
        return portpicker::is_free(self.config.raft_config.raft_api_port as u16)
            && portpicker::is_free(
                self.config
                    .flight_api_address
                    .parse::<SocketAddr>()
                    .expect(&*format!(
                        "cannot parse meta server address {} ",
                        self.config.flight_api_address
                    ))
                    .port(),
            )
            && s.process(pid).is_none();
    }
    // retrieve the configured url for health check
    // TODO(zhihanz): http TLS endpoint
    fn get_health_probe(&self) -> (reqwest::Client, String) {
        let client = reqwest::Client::builder()
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

#[async_trait]
impl LocalRuntime for LocalQueryConfig {
    fn get_pid(&self) -> Option<pid_t> {
        self.pid
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
            .expect("couldn't create stderr file");
        // configure runtime by process local env settings
        // (TODO) S3 configurations
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
            .env(
                databend_query::configs::config_meta::META_ADDRESS,
                conf.meta.meta_address,
            )
            .env(
                databend_query::configs::config_meta::META_USERNAME,
                conf.meta.meta_username,
            )
            .env(
                databend_query::configs::config_meta::META_PASSWORD,
                conf.meta.meta_password,
            )
            .env(
                databend_query::configs::config_storage::STORAGE_TYPE,
                conf.storage.storage_type,
            )
            .env(
                databend_query::configs::config_meta::META_EMBEDDED_DIR,
                "/tmp/embedded",
            )
            .env(
                databend_query::configs::config_storage::DISK_STORAGE_DATA_PATH,
                conf.storage.disk.data_path,
            )
            .env(
                databend_query::configs::config_query::QUERY_HTTP_HANDLER_HOST,
                conf.query.http_handler_host,
            )
            .env(
                databend_query::configs::config_query::QUERY_HTTP_HANDLER_PORT,
                conf.query.http_handler_port.to_string(),
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
    fn is_clean(&self) -> bool {
        if self.pid.is_none() {
            return true;
        }
        let s = System::new_all();
        let pid = self.pid.unwrap();
        portpicker::is_free(self.config.query.mysql_handler_port)
            && portpicker::is_free(self.config.query.clickhouse_handler_port)
            && s.process(pid).is_none()
    }
    // retrieve the configured url for health check
    fn get_health_probe(&self) -> (reqwest::Client, String) {
        let client = reqwest::Client::builder()
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

impl Status {
    pub fn read(conf: Config) -> Result<Self> {
        let status_path = format!("{}/.status.json", conf.databend_dir);
        log::info!("{}", status_path.as_str());
        let local_config_dir = format!("{}/configs/local", conf.databend_dir);
        std::fs::create_dir_all(local_config_dir.as_str())
            .expect("cannot create dir to store local profile");
        if !Path::new(status_path.as_str()).exists() {
            // Create.
            let file = File::create(status_path.as_str())?;
            let status = Status {
                path: status_path.clone(),
                version: "".to_string(),
                local_configs: HashMap::new(),
                local_config_dir,
                current_profile: None,
                mirrors: None,
            };
            serde_json::to_writer(&file, &status)?;
        }

        let file = File::open(status_path)?;
        let reader = BufReader::new(file);
        let status: Status = serde_json::from_reader(reader)?;
        Ok(status)
    }

    pub fn save_local_config<T>(
        status: &mut Status,
        config_type: String,
        file_name: String,
        data: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        if config_type.as_str() != "meta"
            && config_type.as_str() != "query"
            && config_type.as_str() != "dashboard"
        {
            return Err(CliError::Unknown(
                "Unsupported config type for local storage".parse().unwrap(),
            ));
        }
        let file_location = format!("{}/{}", status.local_config_dir, file_name);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_location.clone())?;
        serde_yaml::to_writer(&file, data)
            .expect(&*format!("cannot write to file {}", file_location));
        if status.local_configs.get(&*config_type).is_none() {
            status.local_configs.insert(config_type, file_location);
        } else {
            let current_status = status.clone();
            let mut current_configs: Vec<&str> = current_status
                .local_configs
                .get(&*config_type)
                .unwrap()
                .split(',')
                .filter(|s| !s.is_empty())
                .collect();
            current_configs.push(&*file_location);
            status
                .local_configs
                .insert(config_type, current_configs.join(","));
        }
        status.write()?;
        Ok(())
    }

    pub fn delete_local_config(
        status: &mut Status,
        config_type: String,
        file_name: String,
    ) -> Result<()> {
        if config_type.as_str() != "meta"
            && config_type.as_str() != "query"
            && config_type.as_str() != "dashboard"
        {
            return Err(CliError::Unknown(
                "Unsupported config type for local storage".parse().unwrap(),
            ));
        }
        if status.local_configs.get(config_type.as_str()).is_none() {
            return Ok(());
        }
        if std::fs::remove_file(file_name.clone()).is_ok() {}
        let current_status = status.clone();
        let mut vec = current_status
            .local_configs
            .get(config_type.as_str())
            .unwrap()
            .split(',')
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>();
        vec.retain(|s| *s.to_string() != file_name);

        if vec.len() > 1 {
            status.local_configs.insert(config_type, vec.join(","));
        } else {
            status.local_configs.insert(
                config_type,
                vec.get(0).map_or("".to_string(), |v| v.to_string()),
            );
        }
        status.write()?;
        Ok(())
    }

    pub fn get_local_meta_config(&self) -> Option<(String, LocalMetaConfig)> {
        if self.local_configs.get("meta").is_none()
            || self.local_configs.get("meta").unwrap().is_empty()
        {
            return None;
        }
        let mut meta_file = self.local_configs.get("meta").unwrap().to_string();
        if meta_file.contains(',') {
            let splited = meta_file
                .as_str()
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .collect::<Vec<&str>>();
            if !splited.is_empty() {
                meta_file = splited.get(0).unwrap().to_string()
            }
        };
        if !Path::new(meta_file.as_str()).exists() {
            return None;
        }
        let file =
            File::open(meta_file.to_string()).expect(&*format!("cannot read from {}", meta_file));
        let reader = BufReader::new(file);
        return Some((
            meta_file.to_string(),
            serde_yaml::from_reader(reader).expect(&*format!("cannot read from {}", meta_file)),
        ));
    }

    pub fn get_local_dashboard_config(&self) -> Option<(String, LocalDashboardConfig)> {
        if self.local_configs.get("dashboard").is_none()
            || self.local_configs.get("dashboard").unwrap().is_empty()
        {
            return None;
        }
        let mut dashboard = self.local_configs.get("dashboard").unwrap().to_string();
        if dashboard.contains(',') {
            let splited = dashboard
                .as_str()
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .collect::<Vec<&str>>();
            if !splited.is_empty() {
                dashboard = splited.get(0).unwrap().to_string()
            }
        };
        if !Path::new(dashboard.as_str()).exists() {
            return None;
        }
        let file =
            File::open(dashboard.to_string()).expect(&*format!("cannot read from {}", dashboard));
        let reader = BufReader::new(file);
        return Some((
            dashboard.to_string(),
            serde_yaml::from_reader(reader).expect(&*format!("cannot read from {}", dashboard)),
        ));
    }

    pub fn has_local_configs(&self) -> bool {
        for v in self.local_configs.values() {
            let mut v = v.clone();
            v.retain(|c| c != ',');
            if !v.is_empty() {
                return true;
            }
        }
        false
    }

    pub fn get_local_query_configs(&self) -> Vec<(String, LocalQueryConfig)> {
        if self.local_configs.get("query").is_none() {
            return Vec::new();
        }
        let query_files = self
            .local_configs
            .get("query")
            .unwrap()
            .split(',')
            .collect::<Vec<&str>>();
        let mut ret = Vec::new();
        for file_name in query_files {
            if !Path::new(file_name.to_string().as_str()).exists() {
                continue;
            }
            let file = File::open(file_name.to_string().as_str()).unwrap();
            let reader = BufReader::new(file);
            let values: serde_yaml::Value =
                serde_yaml::from_reader(reader).expect(&*format!("cannot read from {}", file_name));
            let qry = Status::value_to_query_config(&values.clone());
            ret.push((file_name.to_string(), qry.unwrap()));
        }
        ret
    }

    fn value_to_query_config(values: &serde_yaml::Value) -> Result<LocalQueryConfig> {
        let config = QueryConfig {
            config_file: values
                .get("config")
                .and_then(|v| {
                    Option::from(
                        v.get("config_file")
                            .and_then(|val| {
                                Option::from(
                                    serde_yaml::from_value::<String>(val.clone())
                                        .expect("cannot convert config file settingsfrom yaml"),
                                )
                            })
                            .unwrap(),
                    )
                })
                .unwrap(),
            query: values
                .get("config")
                .and_then(|v| {
                    Option::from(
                        v.get("query")
                            .map(|val| {
                                serde_yaml::from_value::<
                                    databend_query::configs::config_query::QueryConfig,
                                >(val.clone())
                                .expect("cannot convert query config from yaml")
                            })
                            .unwrap(),
                    )
                })
                .unwrap(),
            log: values
                .get("config")
                .and_then(|v| {
                    Option::from(
                        v.get("log")
                            .map(|val| {
                                serde_yaml::from_value::<
                                    databend_query::configs::config_log::LogConfig,
                                >(val.clone())
                                .expect("cannot convert log config from yaml")
                            })
                            .unwrap(),
                    )
                })
                .unwrap(),
            meta: values
                .get("config")
                .and_then(|v| {
                    Option::from(
                        v.get("meta")
                            .map(|val| {
                                serde_yaml::from_value::<
                                    databend_query::configs::config_meta::MetaConfig,
                                >(val.clone())
                                .expect("cannot convert meta service config from yaml")
                            })
                            .unwrap(),
                    )
                })
                .unwrap(),
            storage: values
                .get("config")
                .and_then(|v| {
                    Option::from(
                        v.get("storage")
                            .map(|val| {
                                serde_yaml::from_value::<
                                    databend_query::configs::config_storage::StorageConfig,
                                >(val.clone())
                                .expect("cannot convert storage config from yaml")
                            })
                            .unwrap(),
                    )
                })
                .unwrap(),
        };
        Ok(LocalQueryConfig {
            config,
            pid: values
                .get("pid")
                .and_then(|val| val.as_u64().map(|s| s as pid_t)),
            path: values
                .get("path")
                .and_then(|val| val.as_str().and_then(|s| Option::from(s.to_string()))),
            log_dir: values
                .get("log_dir")
                .and_then(|val| val.as_str().and_then(|s| Option::from(s.to_string()))),
        })
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
