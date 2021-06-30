// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use structopt::StructOpt;

use crate::error::Result;

#[derive(StructOpt, Debug)]
pub struct VersionCommand {}

impl VersionCommand {
    pub async fn execute(&self) -> Result<()> {
        Ok(())
    }
}
