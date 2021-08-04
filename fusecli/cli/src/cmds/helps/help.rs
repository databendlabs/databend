// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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

impl Command for HelpCommand {
    fn name(&self) -> &str {
        "help"
    }

    fn about(&self) -> &str {
        "help"
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    fn exec(&self, writer: &mut Writer, _args: String) -> Result<()> {
        for cmd in self.commands.iter() {
            writer.writeln_width(cmd.name(), cmd.about());
        }
        Ok(())
    }
}
