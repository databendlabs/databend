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

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::ArgMatches;
use serde::Deserialize;
use serde::Serialize;

use crate::cmds::clusters::add::AddCommand;
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
        ClusterCommand { conf }
    }

    pub fn default() -> Self {
        // TODO: this function is a hack to avoid the dependency cycle on the Config struct
        // the dependency cycle is avoidable, but we need some work to work around it
        ClusterCommand::create(Config::default())
    }
}

#[async_trait]
impl Command for ClusterCommand {
    fn name(&self) -> &str {
        "cluster"
    }

    fn clap(&self) -> App<'static> {
        let subcommands = self.subcommands();
        let app = App::new("cluster")
            .setting(AppSettings::DisableVersionFlag)
            .about(self.about())
            .subcommand(subcommands[0].clap())
            .subcommand(subcommands[1].clap())
            .subcommand(subcommands[2].clap())
            .subcommand(subcommands[3].clap());
        app
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![
            Arc::new(CreateCommand::create(self.conf.clone())),
            Arc::new(StopCommand::create(self.conf.clone())),
            Arc::new(ViewCommand::create(self.conf.clone())),
            Arc::new(AddCommand::create(self.conf.clone())),
        ]
    }

    fn about(&self) -> &'static str {
        "Cluster life cycle management"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        self.exec_subcommand(writer, args).await
    }
}
