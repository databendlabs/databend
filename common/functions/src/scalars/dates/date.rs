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

use super::now::NowFunction;
use super::number_function::ToMondayFunction;
use super::number_function::ToYearFunction;
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
use crate::scalars::function_factory::FactoryCreatorWithTypes;
use crate::scalars::FunctionFactory;
use crate::scalars::FunctionFeatures;
use crate::scalars::TypedFunctionDescription;

#[derive(Clone)]
pub struct DateFunction {}

impl DateFunction {
    fn round_function_creator(round: u32) -> TypedFunctionDescription {
        let creator: FactoryCreatorWithTypes = Box::new(move |display_name, args| {
            RoundFunction::try_create(display_name, args, round)
        });

        TypedFunctionDescription::creator(creator).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(1),
        )
    }

    pub fn register(factory: &mut FunctionFactory) {
        factory.register_typed("today", TodayFunction::desc());
        factory.register_typed("yesterday", YesterdayFunction::desc());
        factory.register_typed("tomorrow", TomorrowFunction::desc());
        factory.register_typed("now", NowFunction::desc());
        factory.register_typed("toYYYYMM", ToYYYYMMFunction::desc());
        factory.register_typed("toYYYYMMDD", ToYYYYMMDDFunction::desc());
        factory.register_typed("toYYYYMMDDhhmmss", ToYYYYMMDDhhmmssFunction::desc());
        factory.register_typed("toStartOfYear", ToStartOfYearFunction::desc());
        factory.register_typed("toStartOfISOYear", ToStartOfISOYearFunction::desc());
        factory.register_typed("toStartOfQuarter", ToStartOfQuarterFunction::desc());

        factory.register_typed("toStartOfMonth", ToStartOfMonthFunction::desc());
        factory.register_typed("toMonth", ToMonthFunction::desc());
        factory.register_typed("toDayOfYear", ToDayOfYearFunction::desc());
        factory.register_typed("toDayOfMonth", ToDayOfMonthFunction::desc());
        factory.register_typed("toDayOfWeek", ToDayOfWeekFunction::desc());
        factory.register_typed("toHour", ToHourFunction::desc());
        factory.register_typed("toMinute", ToMinuteFunction::desc());
        factory.register_typed("toSecond", ToSecondFunction::desc());
        factory.register_typed("toMonday", ToMondayFunction::desc());
        factory.register_typed("toYear", ToYearFunction::desc());

        // rounders
        factory.register_typed("toStartOfSecond", Self::round_function_creator(1));
        factory.register_typed("toStartOfMinute", Self::round_function_creator(60));
        factory.register_typed("toStartOfFiveMinutes", Self::round_function_creator(5 * 60));
        factory.register_typed("toStartOfTenMinutes", Self::round_function_creator(10 * 60));
        factory.register_typed(
            "toStartOfFifteenMinutes",
            Self::round_function_creator(15 * 60),
        );
        factory.register_typed("timeSlot", Self::round_function_creator(30 * 60));
        factory.register_typed("toStartOfHour", Self::round_function_creator(60 * 60));
        factory.register_typed("toStartOfDay", Self::round_function_creator(60 * 60 * 24));

        factory.register_typed("toStartOfWeek", ToStartOfWeekFunction::desc());

        //interval functions
        factory.register_typed("addYears", AddYearsFunction::desc(1));
        factory.register_typed("addMonths", AddMonthsFunction::desc(1));
        factory.register_typed("addDays", AddDaysFunction::desc(1));
        factory.register_typed("addHours", AddTimesFunction::desc(3600));
        factory.register_typed("addMinutes", AddTimesFunction::desc(60));
        factory.register_typed("addSeconds", AddTimesFunction::desc(1));
        factory.register_typed("subtractYears", AddYearsFunction::desc(-1));
        factory.register_typed("subtractMonths", AddMonthsFunction::desc(-1));
        factory.register_typed("subtractDays", AddDaysFunction::desc(-1));
        factory.register_typed("subtractHours", AddTimesFunction::desc(-3600));
        factory.register_typed("subtractMinutes", AddTimesFunction::desc(-60));
        factory.register_typed("subtractSeconds", AddTimesFunction::desc(-1));
    }
}
