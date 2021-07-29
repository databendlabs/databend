// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::cmds::Writer;
use crate::error::Result;

pub trait Command {
    fn exec(&self, writer: &mut Writer) -> Result<()>;
}
