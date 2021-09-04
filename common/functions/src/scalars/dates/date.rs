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

use super::now::NowFunction;
use super::TodayFunction;
use super::TomorrowFunction;
use super::YesterdayFunction;
use super::ToYYYYFunction;
use crate::scalars::FactoryFuncRef;

#[derive(Clone)]
pub struct DateFunction {}

impl DateFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("today".into(), TodayFunction::try_create);
        map.insert("yesterday".into(), YesterdayFunction::try_create);
        map.insert("tomorrow".into(), TomorrowFunction::try_create);
        map.insert("now".into(), NowFunction::try_create);
        map.insert("toYYYYMM".into(), ToYYYYFunction::try_create);

        Ok(())
    }
}
