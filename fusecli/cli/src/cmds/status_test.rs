// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tempfile::tempdir;

use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

#[test]
fn test_status() -> Result<()> {
    let mut conf = Config::default();

    let t = tempdir()?;
    conf.datafuse_dir = t.path().to_str().unwrap().to_string();

    let mut status = Status::read(conf)?;
    status.version = "xx".to_string();
    status.write()?;

    Ok(())
}
