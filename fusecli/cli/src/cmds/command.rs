// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use dyn_clone::DynClone;

use crate::cmds::Writer;
use crate::error::Result;

pub trait Command: DynClone {
    fn name(&self) -> &str;
    fn about(&self) -> &str;
    fn is(&self, s: &str) -> bool;
    fn exec(&self, writer: &mut Writer) -> Result<()>;
}

dyn_clone::clone_trait_object!(Command);
