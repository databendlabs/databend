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

use std::fs;
use std::io;
use std::io::Write;

use clap::App;
use clap_generate::generate;
use clap_generate::generators::Bash;
use clap_generate::generators::Zsh;
use clap_generate::Generator;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::cmds::command::Command;
use crate::cmds::ClusterCommand;
use crate::cmds::CommentCommand;
use crate::cmds::Config;
use crate::cmds::Env;
use crate::cmds::HelpCommand;
use crate::cmds::PackageCommand;
use crate::cmds::VersionCommand;
use crate::cmds::Writer;
use crate::error::Result;

pub struct Processor {
    env: Env,
    readline: Editor<()>,
    commands: Vec<Box<dyn Command>>,
}

fn print_completions<G: Generator>(app: &mut App) {
    generate::<G, _>(app, app.get_name().to_string(), &mut io::stdout());
}

impl Processor {
    pub fn create(conf: Config) -> Self {
        fs::create_dir_all(conf.databend_dir.clone()).unwrap();

        let sub_commands: Vec<Box<dyn Command>> = vec![
            Box::new(VersionCommand::create()),
            Box::new(CommentCommand::create()),
            Box::new(PackageCommand::create(conf.clone())),
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
        let mut writer = Writer::create();
        match self.env.conf.clone().clap.into_inner().subcommand_name() {
            Some("package") => {
                let cmd = PackageCommand::create(self.env.conf.clone());
                return cmd.exec_match(
                    &mut writer,
                    self.env
                        .conf
                        .clone()
                        .clap
                        .into_inner()
                        .subcommand_matches("package"),
                );
            }
            Some("version") => {
                let cmd = VersionCommand::create();
                cmd.exec(&mut writer, "".parse().unwrap())
            }
            Some("cluster") => {
                let cmd = ClusterCommand::create(self.env.conf.clone());
                return cmd.exec_match(
                    &mut writer,
                    self.env
                        .conf
                        .clone()
                        .clap
                        .into_inner()
                        .subcommand_matches("cluster"),
                );
            }
            Some("completion") => {
                if let Some(generator) = self
                    .env
                    .conf
                    .clone()
                    .clap
                    .into_inner()
                    .subcommand_matches("completion")
                    .unwrap()
                    .value_of("completion")
                {
                    let mut app = Config::build_cli();
                    eprintln!("Generating completion file for {}...", generator);
                    match generator {
                        "bash" => print_completions::<Bash>(&mut app),
                        "zsh" => print_completions::<Zsh>(&mut app),
                        _ => panic!("Unknown generator"),
                    }
                }
                Ok(())
            }
            None => self.process_run_interactive(),
            _ => {
                println!("Some other subcommand was used");
                Ok(())
            }
        }
    }

    pub fn process_run_interactive(&mut self) -> Result<()> {
        let hist_path = format!("{}/history.txt", self.env.conf.databend_dir.clone());
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
            cmd.exec(&mut writer, line.trim().to_string())?;
        } else {
            writeln!(writer, "Unknown command, usage: help").unwrap();
        }
        writer.flush()?;
        Ok(())
    }
}
