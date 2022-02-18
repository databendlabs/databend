// Copyright 2021 Datafuse Labs.
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
use super::number_function::ToMondayFunction;
use super::AddDaysFunction;
use super::AddMonthsFunction;
use super::AddTimesFunction;
use super::AddYearsFunction;
use super::RoundFunction;
use super::ToDayOfMonthFunction;
use super::ToDayOfWeekFunction;
use super::ToDayOfYearFunction;
use super::ToHourFunction;
use super::ToMinuteFunction;
use super::ToMonthFunction;
use super::ToSecondFunction;
use super::ToStartOfISOYearFunction;
use super::ToStartOfMonthFunction;
use super::ToStartOfQuarterFunction;
use super::ToStartOfWeekFunction;
use super::ToStartOfYearFunction;
use super::ToYYYYMMDDFunction;
use super::ToYYYYMMDDhhmmssFunction;
use super::ToYYYYMMFunction;
use super::TodayFunction;
use super::TomorrowFunction;
use super::YesterdayFunction;
use crate::scalars::function_factory::Factory2Creator;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFactory;

#[derive(Clone)]
pub struct DateFunction {}

impl DateFunction {
    fn round_function_creator(round: u32) -> FunctionDescription {
        let creator: Factory2Creator = Box::new(move |display_name| -> Result<Box<dyn Function>> {
            RoundFunction::try_create(display_name, round)
        });

        FunctionDescription::creator(creator).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(1),
        )
    }

    pub fn register2(factory: &mut FunctionFactory) {
        factory.register("today", TodayFunction::desc());
        factory.register("yesterday", YesterdayFunction::desc());
        factory.register("tomorrow", TomorrowFunction::desc());
        factory.register("now", NowFunction::desc());
        factory.register("toYYYYMM", ToYYYYMMFunction::desc());
        factory.register("toYYYYMMDD", ToYYYYMMDDFunction::desc());
        factory.register("toYYYYMMDDhhmmss", ToYYYYMMDDhhmmssFunction::desc());
        factory.register("toStartOfYear", ToStartOfYearFunction::desc());
        factory.register("toStartOfISOYear", ToStartOfISOYearFunction::desc());
        factory.register("toStartOfQuarter", ToStartOfQuarterFunction::desc());

        factory.register("toStartOfMonth", ToStartOfMonthFunction::desc());
        factory.register("toMonth", ToMonthFunction::desc());
        factory.register("toDayOfYear", ToDayOfYearFunction::desc());
        factory.register("toDayOfMonth", ToDayOfMonthFunction::desc());
        factory.register("toDayOfWeek", ToDayOfWeekFunction::desc());
        factory.register("toHour", ToHourFunction::desc());
        factory.register("toMinute", ToMinuteFunction::desc());
        factory.register("toSecond", ToSecondFunction::desc());
        factory.register("toMonday", ToMondayFunction::desc());

        // rounders
        factory.register("toStartOfSecond", Self::round_function_creator(1));
        factory.register("toStartOfMinute", Self::round_function_creator(60));
        factory.register("toStartOfFiveMinutes", Self::round_function_creator(5 * 60));
        factory.register("toStartOfTenMinutes", Self::round_function_creator(10 * 60));
        factory.register(
            "toStartOfFifteenMinutes",
            Self::round_function_creator(15 * 60),
        );
        factory.register("timeSlot", Self::round_function_creator(30 * 60));
        factory.register("toStartOfHour", Self::round_function_creator(60 * 60));
        factory.register("toStartOfDay", Self::round_function_creator(60 * 60 * 24));

        factory.register("toStartOfWeek", ToStartOfWeekFunction::desc());

        //interval functions
        factory.register_arithmetic("addYears", AddYearsFunction::desc(1));
        factory.register_arithmetic("addMonths", AddMonthsFunction::desc(1));
        factory.register_arithmetic("addDays", AddDaysFunction::desc(1));
        factory.register_arithmetic("addHours", AddTimesFunction::desc(3600));
        factory.register_arithmetic("addMinutes", AddTimesFunction::desc(60));
        factory.register_arithmetic("addSeconds", AddTimesFunction::desc(1));
        factory.register_arithmetic("subtractYears", AddYearsFunction::desc(-1));
        factory.register_arithmetic("subtractMonths", AddMonthsFunction::desc(-1));
        factory.register_arithmetic("subtractDays", AddDaysFunction::desc(-1));
        factory.register_arithmetic("subtractHours", AddTimesFunction::desc(-3600));
        factory.register_arithmetic("subtractMinutes", AddTimesFunction::desc(-60));
        factory.register_arithmetic("subtractSeconds", AddTimesFunction::desc(-1));
    }
}
