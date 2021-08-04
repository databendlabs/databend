// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cell::RefCell;

use clap::App;
use clap::AppSettings;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::FetchCommand;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct PackageCommand {
    conf: Config,
    clap: RefCell<App<'static, 'static>>,
}

impl PackageCommand {
    pub fn create(conf: Config) -> Self {
        let clap = RefCell::new(
            App::new("package")
                .setting(AppSettings::ColoredHelp)
                .setting(AppSettings::DisableVersion)
                .setting(AppSettings::DisableHelpSubcommand)
                .subcommand(
                    App::new("fetch")
                        .setting(AppSettings::DisableVersion)
                        .setting(AppSettings::ColoredHelp)
                        .about("Fetch the latest version package"),
                ),
        );
        PackageCommand { conf, clap }
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
        match self
            .clap
            .borrow_mut()
            .clone()
            .get_matches_from_safe(args.split(' '))
        {
            Ok(matches) => match matches.subcommand_name() {
                Some("fetch") => {
                    let fetch = FetchCommand::create(self.conf.clone());
                    fetch.exec(writer, args)?;
                }
                _ => writer.write_err("unknown command, usage: package -h"),
            },
            Err(err) => {
                println!("{}", err);
            }
        }

        Ok(())
    }
}
