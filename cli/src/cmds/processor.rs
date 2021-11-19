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
use std::io::Write;

use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::cmds::command::Command;
use crate::cmds::config::Mode;
use crate::cmds::loads::load::LoadCommand;
use crate::cmds::queries::query::QueryCommand;
use crate::cmds::root::RootCommand;
use crate::cmds::ups::up::UpCommand;
use crate::cmds::ClusterCommand;
use crate::cmds::CommentCommand;
use crate::cmds::Config;
use crate::cmds::Env;
use crate::cmds::HelpCommand;
use crate::cmds::PackageCommand;
use crate::cmds::VersionCommand;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

pub struct Processor {
    env: Env,
    readline: Editor<()>,
    admin_commands: Vec<Box<dyn Command>>,
    comment: CommentCommand,
    help: HelpCommand,
    query: QueryCommand,
}

enum MultilineType {
    SingleQuote,
    DoubleQuote,
    BackSlash,
    None,
}

impl Processor {
    pub fn create(conf: Config) -> Self {
        fs::create_dir_all(conf.databend_dir.clone()).unwrap();

        let admin_commands: Vec<Box<dyn Command>> = vec![
            Box::new(VersionCommand::create()),
            Box::new(PackageCommand::create(conf.clone())),
            Box::new(ClusterCommand::create(conf.clone())),
            Box::new(UpCommand::create(conf.clone())),
            Box::new(LoadCommand::create(conf.clone())),
        ];
        let help_command = HelpCommand::create(admin_commands.clone());
        Processor {
            env: Env::create(conf.clone()),
            readline: Editor::<()>::new(),
            admin_commands,
            comment: CommentCommand::create(),
            help: help_command,
            query: QueryCommand::create(conf),
        }
    }

    pub async fn process_run(&mut self) -> Result<()> {
        let mut writer = Writer::create();
        if let Some(level) = self.env.conf.clap.value_of("log-level") {
            if level != "info" {
                writer.debug = true;
            }
        }

        let cmd = RootCommand::create();
        let args = cmd.clap().get_matches();
        match args.subcommand_name() {
            None => self.process_run_interactive().await,
            Some(_) => cmd.exec_matches(&mut writer, Some(&args)).await,
        }
    }

    pub async fn process_run_interactive(&mut self) -> Result<()> {
        let hist_path = format!("{}/history.txt", self.env.conf.databend_dir.clone());
        let _ = self.readline.load_history(hist_path.as_str());
        let mut content = String::new();
        let mut multiline_type = MultilineType::None;

        loop {
            let mut writer = Writer::create();
            if let Some(level) = self.env.conf.clap.value_of("log-level") {
                if level != "info" {
                    writer.debug = true;
                }
            }
            let prompt = if content.is_empty() {
                self.env.prompt.as_str()
            } else {
                self.env.multiline_prompt.as_str()
            };
            let readline = self.readline.readline(prompt);
            match readline {
                Ok(line) => {
                    let mut line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    match multiline_type {
                        MultilineType::None => {
                            if line.starts_with('\"') {
                                multiline_type = MultilineType::DoubleQuote;
                                content.push_str(&line[1..line.len()]);
                                continue;
                            } else if line.starts_with('\'') {
                                multiline_type = MultilineType::SingleQuote;
                                content.push_str(&line[1..line.len()]);
                                continue;
                            } else if line.ends_with('\\') {
                                content.push_str(&line[0..line.len() - 1]);
                                multiline_type = MultilineType::BackSlash;
                                continue;
                            }
                        }
                        MultilineType::DoubleQuote => {
                            if line.ends_with('\"') {
                                line = &line[0..line.len() - 1]
                            } else {
                                content.push_str(line);
                                continue;
                            }
                        }
                        MultilineType::SingleQuote => {
                            if line.ends_with('\'') {
                                line = &line[0..line.len() - 1]
                            } else {
                                content.push_str(line);
                                continue;
                            }
                        }
                        MultilineType::BackSlash => {
                            if line.ends_with('\\') {
                                content.push_str(&line[0..line.len() - 1]);
                                continue;
                            }
                        }
                    }
                    content.push_str(line);
                    self.readline.history_mut().add(content.clone());
                    match self.processor_line(writer, content.clone()).await {
                        Ok(()) => Ok(()),
                        Err(CliError::Exited) => break,
                        Err(err) => Err(err),
                    }?;
                    content.clear();
                    multiline_type = MultilineType::None;
                }
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    if self.env.conf.mode != Mode::Sql {
                        break;
                    }
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

    pub async fn processor_line(&mut self, mut writer: Writer, line: String) -> Result<()> {
        // mode switch
        if line.to_lowercase().trim().eq("exit") || line.to_lowercase().trim().eq("quit") {
            writeln!(writer, "Bye").unwrap();
            return Err(CliError::Exited);
        }
        if line.to_lowercase().trim().eq("\\sql") {
            writeln!(writer, "Mode switched to SQL query mode").unwrap();
            self.env.load_mode(Mode::Sql);
            return Ok(());
        }
        if line.to_lowercase().trim().eq("\\admin") {
            writeln!(writer, "Mode switched to admin mode").unwrap();
            self.env.load_mode(Mode::Admin);
            return Ok(());
        }

        if self.comment.is(&*line) {
            self.comment
                .exec(&mut writer, line.trim().to_string())
                .await?;
            writer.flush()?;
            return Ok(());
        }
        if self.help.is(&*line) {
            self.help.exec(&mut writer, line.trim().to_string()).await?;
            writer.flush()?;
            return Ok(());
        }
        // query execution mode
        if self.env.conf.mode == Mode::Sql {
            let res = self.query.exec(&mut writer, line.trim().to_string()).await;
            if let Err(e) = res {
                writer.write_err(format!("Cannot exeuction query, if you want to manage databend cluster or check its status, please change to admin mode(type \\admin), error: {:?}", e))
            }
            writer.flush()?;
        } else {
            if let Some(cmd) = self.admin_commands.iter().find(|c| c.is(&*line)) {
                cmd.exec(&mut writer, line.trim().to_string()).await?;
            } else {
                writeln!(writer, "Unknown command, usage: help").unwrap();
            }
            writer.flush()?;
        }
        Ok(())
    }
}
