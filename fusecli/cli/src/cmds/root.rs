// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use structopt::StructOpt;

use crate::cmds::VersionCommand;
use crate::error::Result;

#[derive(StructOpt, Debug)]
pub enum RootCommand {
    Version(VersionCommand),
}

impl RootCommand {
    pub async fn execute(&self) -> Result<()> {
        match self {
            RootCommand::Version(cmd) => cmd.execute().await,
        }
    }
}
