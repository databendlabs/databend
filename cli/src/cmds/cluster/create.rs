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
use clap::value_t;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tar::Archive;

use crate::cmds::{Config, Status};
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::Result;
use crate::cmds::cluster::cluster::ClusterProfile;
use databend_query::configs::QueryConfig;

#[derive(Clone)]
pub struct CreateCommand {
    conf: Config,
}

impl CreateCommand {
    pub fn create(conf: Config) -> Self {
        CreateCommand { conf }
    }
    pub fn generate() -> App<'static, 'static> {
        App::new("create")
            .setting(AppSettings::ColoredHelp)
            .setting(AppSettings::DisableVersion)
            .about("Create a databend cluster based on profile")
            .arg(Arg::with_name("profile").long("profile").help("Profile for deployment, support local and cluster").possible_values(&["local", "cluster"]))
            .arg(Arg::with_name("mysql_handler_port").long("mysql-handler-port").help("Set endpoint to receive mysql queries").env(databend_query::configs::config::QUERY_MYSQL_HANDLER_PORT).default_value("3307"))
            .arg(Arg::with_name("clickhouse_handler_port").long("clickhouse-handler-port").help("Set endpoint to receive clickhouse queries").env(databend_query::configs::config::QUERY_CLICKHOUSE_HANDLER_PORT).default_value("9000"))
            .arg(Arg::with_name("meta_address").long("meta-address").help("Set endpoint to provide metastore service").env(databend_query::configs::config::META_ADDRESS))
            .arg(Arg::with_name("meta_username").long("meta-username").help("Set user name for metastore service authentication").env(databend_query::configs::config::META_USERNAME).default_value("root"))
            .arg(Arg::with_name("meta_password").long("meta-password").help("Set password for metastore service authentication").env(databend_query::configs::config::META_PASSWORD).default_value("root"))
            .arg(Arg::with_name("store_address").long("store-address").help("Set endpoint to provide dfs service").env(databend_query::configs::config::STORE_ADDRESS))
            .arg(Arg::with_name("store_username").long("store-username").help("Set user name for dfs service authentication").env(databend_query::configs::config::STORE_USERNAME).default_value("root"))
            .arg(Arg::with_name("store_password").long("store-password").help("Set password for dfs service authentication").env(databend_query::configs::config::STORE_PASSWORD).default_value("root"))
            .arg(Arg::with_name("log_level").long("log-level").help("Set logging level").env(databend_query::configs::config::LOG_LEVEL).default_value("INFO"))
    }

    fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()>{

        Ok(())
    }

    /// precheck whether current local profile applicable for local host machine
    fn local_exec_precheck(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;

        Ok(())
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                match value_t!(matches, "profile", ClusterProfile) {
                    Ok(val) => {
                        match val {
                            ClusterProfile::Local => {
                                return self.local_exec_match(writer, matches);
                            }
                            ClusterProfile::Cluster => {
                                todo!()
                            }
                        }
                    }
                    Err(E) => {
                        log::error!("{}", E)
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
