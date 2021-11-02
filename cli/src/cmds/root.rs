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
use clap::Arg;
use clap::ArgMatches;

use crate::cmds::command::Command;
use crate::cmds::completions::completion::CompletionCommand;
use crate::cmds::config;
use crate::cmds::config::Config;
use crate::cmds::loads::load::LoadCommand;
use crate::cmds::queries::query::QueryCommand;
use crate::cmds::ups::up::UpCommand;
use crate::cmds::ClusterCommand;
use crate::cmds::PackageCommand;
use crate::cmds::VersionCommand;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct RootCommand {}

impl RootCommand {
    pub fn create() -> Self {
        RootCommand {}
    }
}

#[async_trait]
impl Command for RootCommand {
    fn name(&self) -> &str {
        "bendctl"
    }

    fn clap(&self) -> App<'static> {
        App::new("bendctl")
            .arg(
                Arg::new("group")
                    .long("group")
                    .about("Sets the group name for configuration")
                    .default_value("local")
                    .env("DATABEND_GROUP")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("databend_dir")
                    .long("databend_dir")
                    .about("Sets the directory to store databend binaries(query and store)")
                    .default_value("~/.databend")
                    .env("databend_dir")
                    .global(true)
                    .takes_value(true)
                    .value_hint(clap::ValueHint::DirPath),
            )
            .arg(
                Arg::new("download_url")
                    .long("download_url")
                    .about("Sets the url to download databend binaries")
                    .default_value(config::REPO_DATABEND_URL)
                    .env("DOWNLOAD_URL")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("tag_url")
                    .long("tag_url")
                    .about("Sets the url to for databend tags")
                    .default_value(config::REPO_DATABEND_TAG_URL)
                    .env("DOWNLOAD_URL")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("validation_url")
                    .long("validation_url")
                    .about("Sets the url to validate on custom download network connection")
                    .env("DOWNLOAD_VALIDATION_URL")
                    .default_value(config::REPO_DATABEND_TAG_URL)
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("playground_url")
                    .long("playground_url")
                    .about("Sets the url to download databend playground")
                    .env("DOWNLOAD_PLAYGROUND_URL")
                    .default_value(config::REPO_PLAYGROUND_URL)
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("log-level")
                    .long("log-level")
                    .about("Sets the log-level for current settings")
                    .env("BEND_LOG_LEVEL")
                    .default_value("info")
                    .global(true)
                    .takes_value(true),
            )
            .subcommand(CompletionCommand::default().clap())
            .subcommand(PackageCommand::default().clap())
            .subcommand(VersionCommand::default().clap())
            .subcommand(ClusterCommand::default().clap())
            .subcommand(QueryCommand::default().clap())
            .subcommand(UpCommand::default().clap())
            .subcommand(LoadCommand::default().clap())
    }

    fn about(&self) -> &'static str {
        "Databend CLI"
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        let config = Config::create(self.clap());
        vec![
            Arc::new(PackageCommand::create(config.clone())),
            Arc::new(VersionCommand::create()),
            Arc::new(ClusterCommand::create(config.clone())),
            Arc::new(QueryCommand::create(config.clone())),
            Arc::new(LoadCommand::create(config.clone())),
            Arc::new(UpCommand::create(config)),
            Arc::new(CompletionCommand::create()),
        ]
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        self.exec_subcommand(writer, args).await
    }
}
