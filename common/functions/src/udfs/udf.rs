// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;

use crate::udfs::{ToTypeNameFunction, UdfExampleFunction};
use crate::FactoryFuncRef;

#[derive(Clone)]
pub struct UdfFunction;

impl UdfFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.as_ref().lock();
        map.insert("example", UdfExampleFunction::try_create);
        map.insert("totypename", ToTypeNameFunction::try_create);
        Ok(())
    }
}
