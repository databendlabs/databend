// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::scalars::udfs::exists::ExistsFunction;
use crate::scalars::CrashMeFunction;
use crate::scalars::DatabaseFunction;
use crate::scalars::FactoryFuncRef;
use crate::scalars::SleepFunction;
use crate::scalars::ToTypeNameFunction;
use crate::scalars::UdfExampleFunction;
use crate::scalars::VersionFunction;

#[derive(Clone)]
pub struct UdfFunction;

impl UdfFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("example".into(), UdfExampleFunction::try_create);
        map.insert("totypename".into(), ToTypeNameFunction::try_create);
        map.insert("database".into(), DatabaseFunction::try_create);
        map.insert("version".into(), VersionFunction::try_create);
        map.insert("sleep".into(), SleepFunction::try_create);
        map.insert("crashme".into(), CrashMeFunction::try_create);
        map.insert("exists".into(), ExistsFunction::try_create);
        Ok(())
    }
}
