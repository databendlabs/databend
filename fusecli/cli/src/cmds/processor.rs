// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Write;

use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::cmds::command::Command;
use crate::cmds::VersionCommand;
use crate::cmds::Writer;
use crate::error::Result;

pub struct Processor {
    readline: Editor<()>,
}

impl Processor {
    pub fn create() -> Self {
        Processor {
            readline: Editor::<()>::new(),
        }
    }

    pub fn process_run(&mut self) -> Result<()> {
        loop {
            let writer = Writer::create();
            let readline = self.readline.readline(">> ");
            match readline {
                Ok(line) => {
                    println!("Line: {}", line);
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

    pub fn processor_line(&self, mut writer: Writer, _line: String) -> Result<()> {
        let version = VersionCommand::create();
        version.exec(&mut writer)?;
        writer.flush()?;
        Ok(())
    }
}
