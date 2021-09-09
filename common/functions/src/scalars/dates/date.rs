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
use super::RoundFunction;
use super::ToYYYYMMDDFunction;
use super::ToYYYYMMDDhhmmssFunction;
use super::ToYYYYMMFunction;
use super::TodayFunction;
use super::TomorrowFunction;
use super::YesterdayFunction;
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
        map.insert("toYYYYMM".into(), ToYYYYMMFunction::try_create);
        map.insert("toYYYYMMDD".into(), ToYYYYMMDDFunction::try_create);
        map.insert(
            "toYYYYMMDDhhmmss".into(),
            ToYYYYMMDDhhmmssFunction::try_create,
        );

        // rounders
        {
            map.insert("toStartOfSecond".into(), |display_name| {
                RoundFunction::try_create(display_name, 1)
            });

            map.insert("toStartOfMinute".into(), |display_name| {
                RoundFunction::try_create(display_name, 60)
            });

            map.insert("toStartOfFiveMinutes".into(), |display_name| {
                RoundFunction::try_create(display_name, 5 * 60)
            });

            map.insert("toStartOfTenMinutes".into(), |display_name| {
                RoundFunction::try_create(display_name, 10 * 60)
            });

            map.insert("toStartOfFifteenMinutes".into(), |display_name| {
                RoundFunction::try_create(display_name, 15 * 60)
            });

            map.insert("timeSlot".into(), |display_name| {
                RoundFunction::try_create(display_name, 30 * 60)
            });
            map.insert("toStartOfHour".into(), |display_name| {
                RoundFunction::try_create(display_name, 60 * 60)
            });
            map.insert("toStartOfDay".into(), |display_name| {
                RoundFunction::try_create(display_name, 60 * 60 * 24)
            });
        }

        Ok(())
    }
}
