// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::scalars::CrashMeFunction;
use crate::scalars::DatabaseFunction;
use crate::scalars::FactoryFuncRef;
use crate::scalars::SleepFunction;
use crate::scalars::ToTypeNameFunction;
use crate::scalars::UdfExampleFunction;
use crate::scalars::VersionFunction;
use crate::scalars::udfs::exists::ExistsFunction;

#[derive(Clone)]
pub struct UdfFunction;

impl UdfFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("example", UdfExampleFunction::try_create);
        map.insert("totypename", ToTypeNameFunction::try_create);
        map.insert("database", DatabaseFunction::try_create);
        map.insert("version", VersionFunction::try_create);
        map.insert("sleep", SleepFunction::try_create);
        map.insert("crashme", CrashMeFunction::try_create);
        map.insert("exists", ExistsFunction::try_create);
        Ok(())
    }
}
