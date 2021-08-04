// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs;
use std::io::Write;

use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::cmds::command::Command;
use crate::cmds::ClusterCommand;
use crate::cmds::CommentCommand;
use crate::cmds::Config;
use crate::cmds::Env;
use crate::cmds::FetchCommand;
use crate::cmds::HelpCommand;
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
        fs::create_dir_all(conf.datafuse_dir.clone()).unwrap();

        let sub_commands: Vec<Box<dyn Command>> = vec![
            Box::new(VersionCommand::create()),
            Box::new(CommentCommand::create()),
            Box::new(FetchCommand::create(conf.clone())),
            Box::new(ClusterCommand::create(conf.clone())),
        ];

        let mut commands: Vec<Box<dyn Command>> = sub_commands.clone();
        commands.push(Box::new(HelpCommand::create(sub_commands)));

        Processor {
            env: Env::create(conf),
            readline: Editor::<()>::new(),
            commands,
        }
    }

    pub fn process_run(&mut self) -> Result<()> {
        let hist_path = format!("{}/history.txt", self.env.conf.datafuse_dir.clone());
        let _ = self.readline.load_history(hist_path.as_str());

        loop {
            let writer = Writer::create();
            let readline = self.readline.readline(self.env.prompt.as_str());
            match readline {
                Ok(line) => {
                    self.readline.history_mut().add(line.clone());
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
        self.readline.save_history(hist_path.as_str()).unwrap();
        Ok(())
    }

    pub fn processor_line(&self, mut writer: Writer, line: String) -> Result<()> {
        if let Some(cmd) = self.commands.iter().find(|c| c.is(&*line)) {
            cmd.exec(&mut writer, line)?;
        } else {
            writeln!(writer, "Unknown command").unwrap();
        }
        writer.flush()?;
        Ok(())
    }
}
