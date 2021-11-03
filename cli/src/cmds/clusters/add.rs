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
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use clap::ValueHint;
use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use lexical_util::num::AsPrimitive;
use sysinfo::ProcessExt;
use sysinfo::Signal;
use sysinfo::System;
use sysinfo::SystemExt;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::stop::StopCommand;
use crate::cmds::command::Command;
use crate::cmds::packages::fetch::get_version;
use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalQueryConfig;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::FetchCommand;
use crate::cmds::Status;
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct AddCommand {
    conf: Config,
}

#[derive(Clone)]
pub struct LocalBinaryPaths {
    pub query: String,
    pub meta: String,
}

async fn reconcile_local_meta(status: &mut Status) -> Result<()> {
    let s = System::new_all();
    if let Some((_, meta)) = status.get_local_meta_config() {
        if meta.pid.is_none() || s.process(meta.pid.unwrap()).is_none() {
            return Err(CliError::Unknown(
                "meta service process not found".to_string(),
            ));
        }
        if meta.verify(None, None).await.is_err() {
            return Err(CliError::Unknown("cannot verify meta service".to_string()));
        }
    }
    Ok(())
}

async fn reconcile_local_query(status: &mut Status) -> Result<()> {
    let s = System::new_all();
    for (_, query) in status.get_local_query_configs() {
        if query.pid.is_none() || s.process(query.pid.unwrap()).is_none() {
            return Err(CliError::Unknown(
                "query service process not found".to_string(),
            ));
        }
        if query.verify(None, None).await.is_err() {
            return Err(CliError::Unknown("cannot verify query service".to_string()));
        }
    }
    Ok(())
}

async fn reconcile_local(status: &mut Status) -> Result<()> {
    if status.has_local_configs() {
        if let Err(e) = reconcile_local_meta(status).await {
            return Err(e);
        }
        if let Err(e) = reconcile_local_query(status).await {
            return Err(e);
        }
    }
    Ok(())
}

impl AddCommand {
    pub fn create(conf: Config) -> Self {
        AddCommand { conf }
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(writer, args).await {
            Ok(_) => {
                writer.write_ok("Databend cluster pre-check passed!".to_string());
                // ensuring needed dependencies
                let bin_path = self
                    .ensure_bin(writer, args)
                    .await
                    .expect("cannot find binary path");
                let meta_config = self
                    .generate_local_meta_config(args, bin_path.clone())
                    .expect("Cannot generate meta-service config");
                {
                    let res = self
                        .provision_local_meta_service(writer, meta_config.clone())
                        .await;
                    if res.is_err() {
                        writer.write_err(format!(
                            "Cannot provision meta service, error: {:?}, please check the logs output by: tail -n 100 {}/*",
                            res.unwrap_err(),
                            meta_config.log_dir.unwrap_or_else(|| "unknown log_dir".into()),
                        ));
                        return Ok(());
                    }
                }
                let meta_status = meta_config.verify(None, None).await;
                if meta_status.is_err() {
                    let mut status = Status::read(self.conf.clone())?;
                    writer.write_err(format!(
                        "Cannot connect to meta service: {:?}",
                        meta_status.unwrap_err()
                    ));
                    StopCommand::stop_current_local_services(&mut status, writer)
                        .await
                        .unwrap();
                    return Ok(());
                }
                let query_config = self.generate_local_query_config(args, bin_path, &meta_config);
                if query_config.is_err() {
                    let mut status = Status::read(self.conf.clone())?;
                    writer.write_err(format!(
                        "Cannot generate query configurations, error: {:?}",
                        query_config.as_ref().unwrap_err()
                    ));
                    StopCommand::stop_current_local_services(&mut status, writer)
                        .await
                        .unwrap();
                }
                writer.write_ok(format!(
                    "Local data would be stored in {}",
                    query_config
                        .as_ref()
                        .unwrap()
                        .config
                        .storage
                        .disk
                        .data_path
                        .as_str()
                ));
                {
                    let res = self
                        .provision_local_query_service(
                            writer,
                            query_config.as_ref().unwrap().clone(),
                        )
                        .await;
                    if res.is_err() {
                        let mut status = Status::read(self.conf.clone())?;
                        writer.write_err(format!(
                            "Cannot provison query service, error: {:?}, please check the logs output by: tail -n 100 {}/*",
                            res.unwrap_err(),
                            query_config.as_ref().unwrap().config.log.log_dir,
                        ));
                        StopCommand::stop_current_local_services(&mut status, writer)
                            .await
                            .unwrap();
                    }
                }
                let mut status = Status::read(self.conf.clone())?;
                status.current_profile =
                    Some(serde_json::to_string::<ClusterProfile>(&ClusterProfile::Local).unwrap());
                status.write()?;

                Ok(())
            }
            Err(e) => {
                writer.write_err(format!("Cluster precheck failed, error {:?}", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    async fn local_exec_precheck(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        let mut status = Status::read(self.conf.clone())?;
        if args.is_present("force") {
            writer.write_ok("Delete existing cluster".to_string());
            StopCommand::stop_current_local_services(&mut status, writer)
                .await
                .expect("cannot stop current services");
            let s = System::new_all();
            for elem in s.process_by_name("databend-meta") {
                elem.kill(Signal::Term);
            }
            for elem in s.process_by_name("databend-query") {
                elem.kill(Signal::Term);
            }
            for elem in s.process_by_name("databend-dashboard") {
                elem.kill(Signal::Term);
            }
        }
        if let Err(e) = reconcile_local(&mut status).await {
            writer.write_ok(format!(
                "Local environment has problem {:?}, start reconcile",
                e
            ));
            if let Err(e) = StopCommand::stop_current_local_services(&mut status, writer).await {
                writer.write_err(format!("Cannot delete existing service, error: {:?}", e));
                return Err(e);
            }
        }
        if status.has_local_configs() {
            return Err(CliError::Unknown(format!(
                "❗ found previously existed cluster with config in {}",
                status.local_config_dir
            )));
        }
        let s = System::new_all();

        if !s.process_by_name("databend-meta").is_empty() {
            return Err(CliError::Unknown(
                "❗ have installed databend-meta process before, please stop them and retry"
                    .parse()
                    .unwrap(),
            ));
        }
        if !s.process_by_name("databend-query").is_empty() {
            return Err(CliError::Unknown(
                "❗ have installed databend-query process before, please stop them and retry"
                    .parse()
                    .unwrap(),
            ));
        }
        if args.value_of("meta_address").is_some()
            && !args.value_of("meta_address").unwrap().is_empty()
        {
            let meta_address = SocketAddr::from_str(args.value_of("meta_address").unwrap());
            match meta_address {
                Ok(addr) => {
                    if !portpicker::is_free(addr.port()) {
                        return Err(CliError::Unknown(format!(
                            "Address {} has been used for local meta service",
                            addr.port()
                        )));
                    }
                }
                Err(e) => {
                    return Err(CliError::Unknown(format!(
                        "Cannot parse meta service address, error: {:?}",
                        e
                    )))
                }
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
                    .default_value(""),
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
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "Create a databend cluster based on profile"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let profile = matches.value_of_t("profile");
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
