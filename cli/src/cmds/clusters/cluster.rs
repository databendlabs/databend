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

use std::borrow::Borrow;
use std::str::FromStr;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::ArgMatches;
use serde::Deserialize;
use serde::Serialize;

use crate::cmds::clusters::create::CreateCommand;
use crate::cmds::clusters::stop::StopCommand;
use crate::cmds::clusters::view::ViewCommand;
use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct ClusterCommand {
    conf: Config,
    clap: App<'static>,
}

// Support to up and run databend cluster on different platforms
// For local profile, databend would be deployed as processes in bare metal host
// For cluster profile, databend would be deployed as CRD on kubernetes cluster
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterProfile {
    Local,
    Cluster,
}

// Implement the trait
impl FromStr for ClusterProfile {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<ClusterProfile, &'static str> {
        match s {
            "local" => Ok(ClusterProfile::Local),
            "cluster" => Ok(ClusterProfile::Cluster),
            _ => Err("no match for profile"),
        }
    }
}

impl ClusterCommand {
    pub fn create(conf: Config) -> Self {
        let clap = ClusterCommand::generate();
        ClusterCommand { conf, clap }
    }
    pub fn generate() -> App<'static> {
        let app = App::new("cluster")
            .setting(AppSettings::DisableVersionFlag)
            .about("Cluster life cycle management")
            .subcommand(CreateCommand::generate())
            .subcommand(StopCommand::generate())
            .subcommand(ViewCommand::generate());
        app
    }

    pub(crate) async fn exec_match(
        &self,
        writer: &mut Writer,
        args: Option<&ArgMatches>,
    ) -> Result<()> {
        match args {
            Some(matches) => match matches.subcommand_name() {
                Some("create") => {
                    let create = CreateCommand::create(self.conf.clone());
                    create
                        .exec_match(writer, matches.subcommand_matches("create"))
                        .await?;
                }
                Some("delete") => {
                    let create = StopCommand::create(self.conf.clone());
                    create
                        .exec_match(writer, matches.subcommand_matches("delete"))
                        .await?;
                }
                Some("view") => {
                    let view = ViewCommand::create(self.conf.clone());
                    view.exec_match(writer, matches.subcommand_matches("view"))
                        .await?;
                }
                _ => writer.write_err("unknown command, usage: cluster -h"),
            },
            None => {
                println!("None")
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Command for ClusterCommand {
    fn name(&self) -> &str {
        "cluster"
    }

    fn about(&self) -> &str {
        "Cluster life cycle management"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        match self.clap.clone().try_get_matches_from(args.split(' ')) {
            Ok(matches) => {
                return self.exec_match(writer, Some(matches.borrow())).await;
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}
