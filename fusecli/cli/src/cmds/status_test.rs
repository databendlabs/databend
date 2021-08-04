// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

#[test]
fn test_status() -> Result<()> {
    let mut conf = Config::default();
    conf.datafuse_dir = "/tmp/".to_string();

    let mut status = Status::read(conf)?;
    status.latest = "xx".to_string();
    status.write()?;

    Ok(())
}
