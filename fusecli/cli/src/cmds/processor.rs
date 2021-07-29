// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Write;

use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Env;
use crate::cmds::VersionCommand;
use crate::cmds::Writer;
use crate::error::Result;

pub struct Processor {
    env: Env,
    readline: Editor<()>,
    commands: Vec<Box<dyn Command>>,
}

impl Processor {
    pub fn create(conf: Config) -> Self {
        Processor {
            env: Env::create(conf),
            readline: Editor::<()>::new(),
            commands: vec![Box::new(VersionCommand::create())],
        }
    }

    pub fn process_run(&mut self) -> Result<()> {
        loop {
            let writer = Writer::create();
            let readline = self.readline.readline(self.env.prompt.as_str());
            match readline {
                Ok(line) => {
                    self.processor_line(writer, line)?;
                }
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break;
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn processor_line(&self, mut writer: Writer, line: String) -> Result<()> {
        if let Some(cmd) = self.commands.iter().find(|c| c.is(&*line)) {
            cmd.exec(&mut writer)?;
        } else {
            writeln!(writer, "Unknown command").unwrap();
        }
        writer.flush()?;
        Ok(())
    }
}
