// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use datafuse_cli::cmds::RootCommand;
use datafuse_cli::error::Result;
use structopt::StructOpt;

fn main() -> Result<()> {
    let root: RootCommand = RootCommand::from_args();
    futures::executor::block_on(root.execute())
}
