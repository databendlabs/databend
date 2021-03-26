// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::FuseQueryResult;
use crate::functions::udfs::DatabaseFunction;
use crate::functions::udfs::ToTypeNameFunction;
use crate::functions::udfs::UdfExampleFunction;
use crate::functions::FactoryFuncRef;

#[derive(Clone)]
pub struct UdfFunction;

impl UdfFunction {
    pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("example", UdfExampleFunction::try_create);
        map.insert("totypename", ToTypeNameFunction::try_create);
        map.insert("database", DatabaseFunction::try_create);
        Ok(())
    }
}
