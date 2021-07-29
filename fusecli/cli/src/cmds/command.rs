// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::cmds::Writer;
use crate::error::Result;

pub trait Command {
    fn name(&self) -> &str;
    fn is(&self, s: &str) -> bool;
    fn exec(&self, writer: &mut Writer) -> Result<()>;
}
