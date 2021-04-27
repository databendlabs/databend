// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;

use crate::FactoryFuncRef;
use crate::buildins::DataBaseFunction;


#[derive(Clone)]
pub struct BuildInFunction;

impl BuildInFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("database", DataBaseFunction::try_create);
        Ok(())
    }
}

