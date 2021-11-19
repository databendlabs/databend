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
use clap::ArgMatches;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::FetchCommand;
use crate::cmds::ListCommand;
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct PackageCommand {
    conf: Config,
}

impl PackageCommand {
    pub fn create(conf: Config) -> Self {
        PackageCommand { conf }
    }

    pub fn default() -> Self {
        PackageCommand::create(Config::default())
    }
}

#[async_trait]
impl Command for PackageCommand {
    fn name(&self) -> &str {
        "package"
    }

    fn about(&self) -> &'static str {
        "Package manage databend binary releases"
    }

    fn clap(&self) -> App<'static> {
        let subcommands = self.subcommands();
        let app = App::new("package")
            .about(self.about())
            .subcommand(subcommands[0].clap())
            .subcommand(subcommands[1].clap())
            .subcommand(subcommands[2].clap());
        app
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![
            Arc::new(ListCommand::create(self.conf.clone())),
            Arc::new(FetchCommand::create(self.conf.clone())),
            Arc::new(SwitchCommand::create(self.conf.clone())),
        ]
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        self.exec_subcommand(writer, args).await
    }
}
