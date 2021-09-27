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

use crate::cmds::{Config, Status};
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::{Result, CliError};
use crate::cmds::cluster::cluster::ClusterProfile;
use databend_query::configs::{QueryConfig, StoreConfig};
use databend_dfs::configs::Config as DfsConfig;
use crate::cmds::status::{LocalConfig, LocalStoreConfig};
use std::process::exit;

#[derive(Clone)]
pub struct CreateCommand {
    conf: Config,
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
    }

    fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()>{
        match self.local_exec_precheck(args) {
            Ok(_) => {
                writer.write_ok("databend cluster precheck passed!");
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
