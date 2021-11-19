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
use comfy_table::Cell;
use comfy_table::Row;
use comfy_table::Table;

use crate::cmds::command::Command;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct HelpCommand {
    commands: Vec<Box<dyn Command>>,
}

impl HelpCommand {
    pub fn create(commands: Vec<Box<dyn Command>>) -> Self {
        HelpCommand { commands }
    }
}

#[async_trait]
impl Command for HelpCommand {
    fn name(&self) -> &str {
        "help"
    }

    fn clap(&self) -> App<'static> {
        App::new("help").about(self.about())
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "show help"
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    async fn exec_matches(&self, writer: &mut Writer, _args: Option<&ArgMatches>) -> Result<()> {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        // Title.
        table.set_header(vec![Cell::new("Name"), Cell::new("About")]);

        for cmd in self.commands.iter() {
            table.add_row(Row::from([cmd.name(), cmd.about()]));
        }
        writer.write_ok("Mode switch commands:".to_string());
        writer.writeln_width(
            "\\sql",
            "Switch to query mode, you could run query directly under this mode",
        );
        writer.writeln_width(
            "\\admin",
            "Switch to cluster administration mode, you could profile/view/update databend cluster",
        );
        writer.write_ok("Admin commands:".to_string());
        writer.writeln(&table.trim_fmt());
        Ok(())
    }
}
