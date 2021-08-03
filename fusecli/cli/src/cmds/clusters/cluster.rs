// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use ansi_term::Colour::Green;
use ansi_term::Colour::Red;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct ClusterCommand {
    conf: Config,
}

impl ClusterCommand {
    pub fn create(conf: Config) -> Self {
        ClusterCommand { conf }
    }
}

impl Command for ClusterCommand {
    fn name(&self) -> &str {
        "cluster"
    }

    fn about(&self) -> &str {
        "Start the fuse-query and fuse-store service"
    }

    fn is(&self, s: &str) -> bool {
        s.starts_with(self.name())
    }

    fn exec(&self, writer: &mut Writer, _args: String) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        let bin_dir = format!("{}/bin/{}", self.conf.datafuse_dir, status.latest);

        // Start query-query.
        let cmd = format!(r#"{}/fuse-query > /tmp/query-log.txt 2>&1 &"#, bin_dir);
        writer.writeln(
            format!("Start [{}]", Green.paint("fuse-query")).as_str(),
            cmd.as_str(),
        );
        let (_, _, e) = run_script::run_script!(cmd)?;
        if !e.is_empty() {
            writer.writeln("Error", format!("{}", Red.paint(e)).as_str());
        } else {
            writer.writeln(format!("[{}]", Green.paint("OK")).as_str(), "");
        }

        // Start query-query.
        let cmd = format!(r#"{}/fuse-store> /tmp/store-log.txt 2>&1 &"#, bin_dir);
        writer.writeln(
            format!("Start [{}]", Green.paint("fuse-store")).as_str(),
            cmd.as_str(),
        );
        let (_, _, e) = run_script::run_script!(cmd)?;
        if !e.is_empty() {
            writer.writeln("Error", format!("{}", Red.paint(e)).as_str());
        } else {
            writer.writeln(format!("[{}]", Green.paint("OK")).as_str(), "");
        }
        Ok(())
    }
}
