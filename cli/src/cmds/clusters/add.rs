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

use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use clap::ValueHint;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::create::generate_local_log_dir;
use crate::cmds::clusters::create::generate_local_query_config;
use crate::cmds::clusters::create::get_bin;
use crate::cmds::clusters::create::provision_local_query_service;
use crate::cmds::clusters::utils::get_profile;
use crate::cmds::clusters::view::poll_health;
use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct AddCommand {
    conf: Config,
}

impl AddCommand {
    pub fn create(conf: Config) -> Self {
        AddCommand { conf }
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args).await {
            Ok(_) => {
                writer.write_ok("cluster add precheck passed".to_string());
                let mut status = Status::read(self.conf.clone())?;
                let bin_path = get_bin(&self.conf, &status)?;
                let (_, meta) = status.get_local_meta_config().unwrap();
                let local_log_dir = generate_local_log_dir(
                    &self.conf,
                    format!("local_query_log_{}", status.get_local_query_configs().len()).as_str(),
                );
                let local_query = generate_local_query_config(
                    self.conf.clone(),
                    args,
                    bin_path,
                    &meta,
                    local_log_dir,
                )?;
                let file_name = format!(
                    "query_config_{}.yaml",
                    status.get_local_query_configs().len()
                );
                if let Err(e) = provision_local_query_service(
                    &mut status,
                    writer,
                    local_query,
                    file_name.clone(),
                )
                .await
                {
                    writer.write_err(format!("Cannot provision query service, error: {:?}", e));
                }
                writer.write_ok(format!(
                    "successfully added query instance {} on local",
                    file_name
                ));
                Ok(())
            }
            Err(e) => {
                writer.write_err(format!("{:?}", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    async fn local_exec_precheck(&self, args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        //check on cluster status
        let (_, meta, _) = poll_health(&ClusterProfile::Local, &status, None, None).await;
        if meta.is_none() || meta.unwrap().1.is_err() {
            return Err(CliError::Unknown(
                "current meta service is deleted or unavailable".to_string(),
            ));
        }
        if status.version.is_empty() {
            return Err(CliError::Unknown(
                "no existing version exists, please create a cluster at first".to_string(),
            ));
        }
        if let Some(port) = args.value_of("mysql_handler_port") {
            if !portpicker::is_free(port.parse().unwrap()) {
                return Err(CliError::Unknown(format!(
                    "mysql handler port {} is used by other processes",
                    port
                )));
            }
        }
        if let Some(port) = args.value_of("clickhouse_handler_port") {
            if !portpicker::is_free(port.parse().unwrap()) {
                return Err(CliError::Unknown(format!(
                    "clickhouse handler port {} is used by other processes",
                    port
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Command for AddCommand {
    fn name(&self) -> &str {
        "add"
    }

    fn clap(&self) -> App<'static> {
        App::new("add")
            .setting(AppSettings::DisableVersionFlag)
            .about(self.about())
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile for deployment, support local and cluster")
                    .required(false)
                    .takes_value(true)
                    .possible_values(&["local"]).default_value("local"),
            )
            .arg(
                Arg::new("log_level")
                    .long("log-level")
                    .about("Set logging level")
                    .takes_value(true)
                    .env(databend_query::configs::config_log::LOG_LEVEL)
                    .default_value("INFO"),
            )
            .arg(
                Arg::new("num_cpus")
                    .long("num-cpus")
                    .env(databend_query::configs::config_query::QUERY_NUM_CPUS)
                    .takes_value(true)
                    .about("Set number of cpus for query instance to use")
                    .default_value("2"),
            )
            .arg(
                Arg::new("query_namespace")
                    .long("query-namespace")
                    .env(databend_query::configs::config_query::QUERY_NAMESPACE)
                    .takes_value(true)
                    .about("Set the namespace for query to work on")
                    .default_value("test_cluster"),
            )
            .arg(
                Arg::new("query_tenant")
                    .long("query-tenant")
                    .env(databend_query::configs::config_query::QUERY_TENANT)
                    .takes_value(true)
                    .about("Set the tenant for query to work on")
                    .default_value("test"),
            )
            .arg(
                Arg::new("version")
                    .long("version")
                    .takes_value(true)
                    .about("Set databend version to run")
            )
            .arg(
                Arg::new("storage_type")
                    .long("storage-type")
                    .takes_value(true)
                    .env(databend_query::configs::config_storage::STORAGE_TYPE)
                    .about("Set the storage medium to store datasets, support disk or s3 object storage ")
                    .possible_values(&["disk", "s3"]).default_value("disk"),
            )
            .arg(
                Arg::new("disk_path")
                    .long("disk-path")
                    .takes_value(true)
                    // .env(databend_query::configs::config_storage::DISK_STORAGE_DATA_PATH)
                    .about("Set the root directory to store all datasets")
                    .value_hint(ValueHint::DirPath),
            )
            .arg(
                Arg::new("mysql_handler_port")
                    .long("mysql-handler-port")
                    .takes_value(true)
                    .env(databend_query::configs::config_query::QUERY_MYSQL_HANDLER_PORT)
                    .about("Configure the port for mysql endpoint to run queries in mysql client"),
            )
            .arg(
                Arg::new("clickhouse_handler_port")
                    .long("clickhouse-handler-port")
                    .env(databend_query::configs::config_query::QUERY_CLICKHOUSE_HANDLER_HOST)
                    .takes_value(true)
                    .about("Configure the port clickhouse endpoint to run queries in clickhouse client"),
            )
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "Add a query instance on existing cluster"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let status = Status::read(self.conf.clone())?;
                let profile = get_profile(status, matches.value_of("profile"));
                match profile {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches).await;
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(_) => writer
                        .write_err("Currently profile only support cluster or local".to_string()),
                }
            }
            None => {
                println!("none ");
            }
        }

        Ok(())
    }
}
