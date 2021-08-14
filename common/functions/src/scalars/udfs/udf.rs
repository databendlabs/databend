// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
