// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use rustyline::error::ReadlineError;
use rustyline::Editor;

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

    pub fn process_line(&self) -> Result<()> {
        Ok(())
    }

    pub fn process_run(&mut self) -> Result<()> {
        loop {
            let readline = self.readline.readline(">> ");
            match readline {
                Ok(line) => {
                    println!("Line: {}", line);
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
}
