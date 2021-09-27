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

use std::borrow::Borrow;

use clap::App;
use clap::AppSettings;
use clap::Arg;
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
    clap: App<'static, 'static>,
}

impl PackageCommand {
    pub fn create(conf: Config) -> Self {
        let clap = PackageCommand::generate();
        PackageCommand { conf, clap }
    }
    pub fn generate() -> App<'static, 'static> {
        return App::new("package")
            .setting(AppSettings::ColoredHelp)
            .about("Package manage databend binary releases")
            .subcommand(
                App::new("fetch")
                    .setting(AppSettings::ColoredHelp)
                    .about("Fetch the given version binary package")
                    .arg(Arg::with_name("version").help("Version of databend package to fetch").default_value("latest")),
            )
            .subcommand(
                App::new("list")
                    .setting(AppSettings::ColoredHelp)
                    .about("List all the packages"),
            )
            .subcommand(
                App::new("switch")
                    .setting(AppSettings::ColoredHelp)
                    .about("Switch the active databend to a specified version")
                    .arg(Arg::with_name("version").required(true).help(
                        "Version of databend package, e.g. v0.4.69-nightly. Check the versions: package list"
                    ))
            );
    }

    pub(crate) fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => match matches.subcommand_name() {
                Some("fetch") => {
                    let fetch = FetchCommand::create(self.conf.clone());
                    fetch.exec_match(writer, matches.subcommand_matches("fetch"))?;
                }
                Some("list") => {
                    let list = ListCommand::create(self.conf.clone());
                    list.exec_match(writer, matches.subcommand_matches("list"))?;
                }
                Some("switch") => {
                    let switch = SwitchCommand::create(self.conf.clone());
                    switch.exec_match(writer, matches.subcommand_matches("switch"))?;
                }
                _ => writer.write_err("unknown command, usage: package -h"),
            },
            None => {
                println!("None")
            }
        }

        Ok(())
    }
}

impl Command for PackageCommand {
    fn name(&self) -> &str {
        "package"
    }

    fn about(&self) -> &str {
        "Package command"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        match self.clap.clone().get_matches_from_safe(args.split(' ')) {
            Ok(matches) => {
                return self.exec_match(writer, Some(matches.borrow()));
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}
