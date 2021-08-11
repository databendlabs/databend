// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs;

use crate::cmds::Config;
use crate::cmds::ListCommand;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct SwitchCommand {
    conf: Config,
}

impl SwitchCommand {
    pub fn create(conf: Config) -> Self {
        SwitchCommand { conf }
    }

    pub fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        let bin_dir = format!("{}/bin", self.conf.datafuse_dir.clone());
        let paths = fs::read_dir(bin_dir)?;

        let mut exists = false;
        for path in paths {
            let path = path.unwrap().path();
            let version = path.file_name().unwrap().to_string_lossy().into_owned();
            if version == args {
                exists = true;
                break;
            }
        }

        if !exists {
            writer.write_err(format!("Can't found version: {}, package list:", args).as_str());
            let list = ListCommand::create(self.conf.clone());
            list.exec(writer, args)?;
            return Ok(());
        }

        // Write to status.
        let mut status = Status::read(self.conf.clone())?;
        status.version = args.clone();
        status.write()?;

        writer.write_ok(format!("Package switch to {}", args).as_str());

        Ok(())
    }
}
