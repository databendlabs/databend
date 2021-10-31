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

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::utils;
use crate::cmds::command::Command;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct StopCommand {
    conf: Config,
}

impl StopCommand {
    pub fn create(conf: Config) -> Self {
        StopCommand { conf }
    }

    pub async fn stop_current_local_services(
        status: &mut Status,
        writer: &mut Writer,
    ) -> Result<()> {
        writer.write_warn("Start to clean up local services".to_string());
        for (fs, query) in status.get_local_query_configs() {
            if query.kill().await.is_err() {
                if Status::delete_local_config(status, "query".to_string(), fs.clone()).is_err() {
                    writer.write_err(format!("Cannot clean query config in {}", fs.clone()))
                }
                writer.write_err(format!(
                    "Cannot kill query service with config in {}",
                    fs.clone()
                ))
            }

            if Status::delete_local_config(status, "query".to_string(), fs.clone()).is_err() {
                writer.write_err(format!("cannot clean query config in {}", fs.clone()))
            }
            writer.write_warn(format!("Stopped query service with config in {}", fs));
        }
        if status.get_local_meta_config().is_some() {
            let (fs, meta) = status.get_local_meta_config().unwrap();
            if meta.kill().await.is_err() {
                writer.write_err(format!("Cannot kill meta service with config in {}", fs));
                if Status::delete_local_config(status, "meta".to_string(), fs.clone()).is_err() {
                    writer.write_err(format!("Cannot clean meta config in {}", fs))
                }
            }
            Status::delete_local_config(status, "meta".to_string(), fs.clone())
                .expect("cannot clean meta config");
            writer.write_warn(format!("Stopped meta service with config in {}", fs));
        }
        if status.get_local_dashboard_config().is_some() {
            let (fs, dash) = status.get_local_dashboard_config().unwrap();
            if dash.kill().await.is_err() {
                writer.write_err(format!(
                    "Cannot kill dashboard service with config in {}",
                    fs
                ));
                if Status::delete_local_config(status, "meta".to_string(), fs.clone()).is_err() {
                    writer.write_err(format!("Cannot clean meta config in {}", fs))
                }
            }
            Status::delete_local_config(status, "dashboard".to_string(), fs.clone())
                .expect("cannot clean meta config");
            writer.write_warn(format!("Stopped meta service with config in {}", fs));
        }
        Ok(())
    }

    async fn local_exec_match(&self, writer: &mut Writer, _args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck().await {
            Ok(_) => {
                let mut status = Status::read(self.conf.clone())?;
                if let Err(e) = StopCommand::stop_current_local_services(&mut status, writer).await
                {
                    writer.write_err(format!("{:?}", e));
                };
                status.current_profile = None;
                status.write()?;
                writer.write_rocket("Stopped all services".to_string());
            }
            Err(e) => {
                writer.write_err(format!("{:?}", e));
            }
        }
        //(TODO) purge semantics
        Ok(())
    }

    // precheck delete commands
    async fn local_exec_precheck(&self) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        if status.current_profile.is_none() {
            return Err(CliError::Unknown(format!(
                "❗ No current profile exists in {}",
                status.local_config_dir
            )));
        }
        if !status.has_local_configs() {
            return Err(CliError::Unknown(format!(
                "❗ Does not have local config in {}",
                status.local_config_dir
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl Command for StopCommand {
    fn name(&self) -> &str {
        "stop"
    }

    fn clap(&self) -> App<'static> {
        App::new("stop")
            .setting(AppSettings::DisableVersionFlag)
            .about(self.about())
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile to delete, support local and clusters")
                    .required(false)
                    .takes_value(true),
            )
            .arg(
                Arg::new("purge")
                    .long("purge")
                    .takes_value(false)
                    .about("Purge would delete both persist data and deploy instances"),
            )
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "Delete a databend cluster (delete current cluster by default)"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let status = Status::read(self.conf.clone())?;
                let p = utils::get_profile(status, matches.value_of("profile"));
                match p {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches).await
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(e) => writer.write_err(format!("Cannot parse profile, {:?}", e)),
                }
            }
            None => {
                writer.write_err("Cannot find matches for cluster delete".to_string());
            }
        }

        Ok(())
    }
}
