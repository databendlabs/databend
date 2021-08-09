// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use datafuse_cli::cmds::Config;
use datafuse_cli::cmds::Processor;
use datafuse_cli::error::Result;

fn main() -> Result<()> {
    let conf = Config::create();
    let mut processor = Processor::create(conf);
    processor.process_run()
}
