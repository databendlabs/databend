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

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use clap::{ArgMatches, App, AppSettings, Arg};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tar::Archive;

use crate::cmds::{Config, Status, FetchCommand};
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::{Result, CliError};
use crate::cmds::cluster::cluster::ClusterProfile;
use databend_query::configs::{QueryConfig, StoreConfig};
use databend_dfs::configs::Config as DfsConfig;
use crate::cmds::status::{LocalConfig, LocalStoreConfig, LocalRuntime};
use std::process::exit;

#[derive(Clone)]
pub struct CreateCommand {
    conf: Config,
}
struct LocalBinaryPaths {
    query: String,
    store: String,
}
impl CreateCommand {
    pub fn create(conf: Config) -> Self {
        CreateCommand { conf }
    }
    pub fn generate() -> App<'static> {
        App::new("create")
            .setting(AppSettings::ColoredHelp)
            .setting(AppSettings::DisableVersionFlag)
            .about("Create a databend cluster based on profile")
            .arg(Arg::new("profile").long("profile").about("Profile for deployment, support local and cluster").required(true).possible_values(&["local"]))
            .arg(Arg::new("mysql_handler_port").long("mysql-handler-port").about("Set endpoint to receive mysql queries").env(databend_query::configs::config::QUERY_MYSQL_HANDLER_PORT).default_value("3307"))
            .arg(Arg::new("clickhouse_handler_port").long("clickhouse-handler-port").about("Set endpoint to receive clickhouse queries").env(databend_query::configs::config::QUERY_CLICKHOUSE_HANDLER_PORT).default_value("9000"))
            .arg(Arg::new("meta_address").long("meta-address").about("Set endpoint to provide metastore service").env(databend_query::configs::config::META_ADDRESS))
            .arg(Arg::new("meta_username").long("meta-username").about("Set user name for metastore service authentication").env(databend_query::configs::config::META_USERNAME).default_value("root"))
            .arg(Arg::new("meta_password").long("meta-password").about("Set password for metastore service authentication").env(databend_query::configs::config::META_PASSWORD).default_value("root"))
            .arg(Arg::new("store_address").long("store-address").about("Set endpoint to provide dfs service").env(databend_query::configs::config::STORE_ADDRESS))
            .arg(Arg::new("store_username").long("store-username").about("Set user name for dfs service authentication").env(databend_query::configs::config::STORE_USERNAME).default_value("root"))
            .arg(Arg::new("store_password").long("store-password").about("Set password for dfs service authentication").env(databend_query::configs::config::STORE_PASSWORD).default_value("root"))
            .arg(Arg::new("log_level").long("log-level").about("Set logging level").env(databend_query::configs::config::LOG_LEVEL).default_value("INFO"))
            .arg(Arg::new("version").long("version").about("Set databend version to run").default_value("latest"))
    }

    fn binary_path(&self, dir: String, version: String, name: String) -> Result<String> {
        if version.is_empty() {
            return Err(CliError::Unknown("cannot find binary path, current version is empty".to_string()));
        }
        let bin_path = format!("{}/{}/{}", dir, version, name);
        if  !Path::new(bin_path.as_str()).exists() {
            return Err(CliError::Unknown(format!("cannot find binary path in {}", bin_path)))
        }
        Ok(fs::canonicalize(&bin_path).unwrap().to_str().unwrap().to_string())
    }

    fn ensure_bin(&self, writer: &mut Writer, args: &ArgMatches) -> Result<LocalBinaryPaths> {
        let status = Status::read(self.conf.clone())?;

        let mut paths = LocalBinaryPaths{ query: "".to_string(), store: "".to_string() };
        if self.binary_path(format!("{}/bin", self.conf.databend_dir), status.version.clone(), "databend-query".to_string()).is_err() {
            // fetch latest version of databend binary if version not found
            writer.write_ok(&*format!("Cannot find databend binary path in version {}, start to download", args.value_of("version").unwrap()));
            FetchCommand::create(self.conf.clone()).exec_match(writer, Some(args))?;
        }
       SwitchCommand::create(self.conf.clone()).exec_match(writer, Some(args))?;
        let status = Status::read(self.conf.clone())?;
        paths.query = self.binary_path(format!("{}/bin", self.conf.databend_dir), status.version.clone(), "databend-query".to_string()).expect("cannot find query bin");
        paths.store = self.binary_path(format!("{}/bin", self.conf.databend_dir), status.version, "databend-store".to_string()).expect("cannot find store bin");
        return Ok(paths)
    }

    fn generate_dfs_config() -> DfsConfig {
        let mut config = DfsConfig::empty();
        if !portpicker::is_free(config.metric_api_address.parse().unwrap()) {
            config.metric_api_address = Status::find_unused_local_port()
        }

        if !portpicker::is_free(config.http_api_address.parse().unwrap()) {
            config.http_api_address = Status::find_unused_local_port()
        }
        if !portpicker::is_free(config.flight_api_address.parse().unwrap()) {
            config.flight_api_address = Status::find_unused_local_port()
        }
        return config
    }
    fn generate_local_meta_config(&self, args: &ArgMatches, bin_path: LocalBinaryPaths) -> Option<LocalStoreConfig> {
        let mut config = DfsConfig::empty();

        if args.value_of("meta_address").is_some() && args.value_of("meta_address").unwrap().is_empty() {
            config.flight_api_address = args.value_of("meta_address").unwrap().to_string();
        }
        config.log_level = args.value_of("log_level").unwrap().to_string();
        let log_base = format!("{}/logs", self.conf.clone().databend_dir);
        if !Path::new(log_base.as_str()).exists() {
            fs::create_dir(Path::new(log_base.as_str())).expect(format!("cannot create directory {}", log_base).as_str());
        }
        let meta_log_dir = format!("{}/local_meta_log", log_base);
        if !Path::new(meta_log_dir.as_str()).exists() {
            fs::create_dir(Path::new(meta_log_dir.as_str())).expect(format!("cannot create directory {}", meta_log_dir).as_str());
        }
        config.log_dir = meta_log_dir;

        let data_base = format!("{}/data", self.conf.clone().databend_dir);
        if !Path::new(data_base.as_str()).exists() {
            fs::create_dir(Path::new(data_base.as_str())).expect(format!("cannot create directory {}", data_base).as_str());
        }
        let meta_data_dir = format!("{}/_meta_local_fs", data_base);
        if !Path::new(meta_data_dir.as_str()).exists() {
            fs::create_dir(Path::new(meta_data_dir.as_str())).expect(format!("cannot create directory {}", meta_data_dir).as_str());
        }
        config.local_fs_dir = meta_data_dir;
        config.meta_config.single = true;
        return Some(LocalStoreConfig{
            config,
            pid: None,
            path: Some(bin_path.store),
            log_dir: Some(log_base)
        })
    }

    fn provision_local_meta_service(&self, writer: &mut Writer, args: &ArgMatches, bin_path: LocalBinaryPaths) -> Result<()> {
        let mut meta_config = self.generate_local_meta_config(args, bin_path).expect("cannot generate metaservice config");
        match meta_config.start() {
            Ok(_) => {
                assert!(meta_config.get_pid().is_some());
                let mut status = Status::read(self.conf.clone())?;
                status.local_configs.meta_configs = Some(meta_config.clone());
                status.write()?;
                writer.write_ok(format!("👏 successfully started meta service with rpc endpoint {}", meta_config.config.flight_api_address).as_str());
                return Ok(())
            }
            Err(e) => {
                return Err(e)
            }
        }

    }

    fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()>{
        match self.local_exec_precheck(args) {
            Ok(_) => {
                writer.write_ok("databend cluster precheck passed!");
                /// ensuring needed dependencies
                let bin_path = self.ensure_bin(writer, args).expect("cannot find binary path");
                self.provision_local_meta_service(writer, args, bin_path);
                Ok(())
            }
            Err(CliError::Unknown(s)) => {
                writer.write_err(s.as_str());
                return Ok(())
            }
            Err(E) => {
                log::error!("{:?}", E);
                return Ok(())
            }
        }

    }

    /// precheck whether current local profile applicable for local host machine
    fn local_exec_precheck(&self, args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        if status.local_configs !=  LocalConfig::empty() {
            return Err(CliError::Unknown(format!("❗ found previously existed cluster with config in {}", status.path).to_string()))
        }
        if !portpicker::is_free(args.value_of("mysql_handler_port").unwrap().parse().unwrap()) {
            return Err(CliError::Unknown(format!("mysql handler port {} is occupied by other program", args.value_of("mysql_handler_port").unwrap()).to_string()))
        }
        if !portpicker::is_free(args.value_of("clickhouse_handler_port").unwrap().parse().unwrap()) {
            return Err(CliError::Unknown(format!("clickhouse handler port {} is occupied by other program", args.value_of("clickhouse_handler_port").unwrap()).to_string()))
        }
        if args.value_of("meta_address").is_some() && !args.value_of("meta_address").unwrap().is_empty() && !portpicker::is_free(args.value_of("meta_address").unwrap().parse().unwrap()) {
            return Err(CliError::Unknown(format!("clickhouse handler port {} is occupied by other program", args.value_of("clickhouse_handler_port").unwrap()).to_string()))
        }
        if args.value_of("store_address").is_some() && !args.value_of("store_address").unwrap().is_empty() && !portpicker::is_free(args.value_of("store_address").unwrap().parse().unwrap()) {
            return Err(CliError::Unknown(format!("clickhouse handler port {} is occupied by other program", args.value_of("clickhouse_handler_port").unwrap()).to_string()))
        }

        Ok(())
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let profile= matches.value_of_t("profile");
                match profile {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches);
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(_) => {
                        writer.write_err("currently profile only support cluster or local")
                    }
                }

            }
            None => {
                println!("none ");
            }
        }

        Ok(())
    }

}
