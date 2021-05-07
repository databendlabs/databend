// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::udfs::DatabaseFunction;
use crate::udfs::SubstringFunction;
use crate::udfs::ToTypeNameFunction;
use crate::udfs::UdfExampleFunction;
use crate::FactoryFuncRef;

#[derive(Clone)]
pub struct UdfFunction;

impl UdfFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("example", UdfExampleFunction::try_create);
        map.insert("totypename", ToTypeNameFunction::try_create);
        map.insert("database", DatabaseFunction::try_create);
        map.insert("substring", SubstringFunction::try_create);
        Ok(())
    }
}
