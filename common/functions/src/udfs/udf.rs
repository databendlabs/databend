// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::udfs::{ToTypeNameFunction, UdfExampleFunction};
use crate::{FactoryFuncRef, FunctionResult};

#[derive(Clone)]
pub struct UdfFunction;

impl UdfFunction {
    pub fn register(map: FactoryFuncRef) -> FunctionResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("example", UdfExampleFunction::try_create);
        map.insert("totypename", ToTypeNameFunction::try_create);
        Ok(())
    }
}
