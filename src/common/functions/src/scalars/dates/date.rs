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

use common_datavalues::IntervalKind;

use super::now::NowFunction;
use super::number_function::ToMondayFunction;
use super::number_function::ToYearFunction;
use super::round_function::Round;
use super::to_interval_function_creator;
use super::AddDaysFunction;
use super::AddMonthsFunction;
use super::AddTimesFunction;
use super::AddYearsFunction;
use super::DateAddFunction;
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
use crate::scalars::function_factory::FactoryCreator;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFactory;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct DateFunction {}

impl DateFunction {
    fn round_function_creator(round: Round) -> FunctionDescription {
        let creator: FactoryCreator = Box::new(move |display_name, args| {
            RoundFunction::try_create(display_name, args, round)
        });

        FunctionDescription::creator(creator).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(1),
        )
    }

    pub fn register(factory: &mut FunctionFactory) {
        factory.register("today", TodayFunction::desc());
        factory.register("yesterday", YesterdayFunction::desc());
        factory.register("tomorrow", TomorrowFunction::desc());
        factory.register("now", NowFunction::desc());
        factory.register("to_yyyymm", ToYYYYMMFunction::desc());
        factory.register("to_yyyymmdd", ToYYYYMMDDFunction::desc());
        factory.register("to_yyyymmddhhmmss", ToYYYYMMDDhhmmssFunction::desc());
        factory.register("to_start_of_year", ToStartOfYearFunction::desc());
        factory.register("to_start_of_iso_year", ToStartOfISOYearFunction::desc());
        factory.register("to_start_of_quarter", ToStartOfQuarterFunction::desc());

        factory.register("to_start_of_month", ToStartOfMonthFunction::desc());
        factory.register("to_month", ToMonthFunction::desc());
        factory.register("to_day_of_year", ToDayOfYearFunction::desc());
        factory.register("to_day_of_month", ToDayOfMonthFunction::desc());
        factory.register("to_day_of_week", ToDayOfWeekFunction::desc());
        factory.register("to_hour", ToHourFunction::desc());
        factory.register("to_minute", ToMinuteFunction::desc());
        factory.register("to_second", ToSecondFunction::desc());
        factory.register("to_monday", ToMondayFunction::desc());
        factory.register("to_year", ToYearFunction::desc());

        // rounders
        factory.register(
            "to_start_of_second",
            Self::round_function_creator(Round::Second),
        );
        factory.register(
            "to_start_of_minute",
            Self::round_function_creator(Round::Minute),
        );
        factory.register(
            "to_start_of_five_minutes",
            Self::round_function_creator(Round::FiveMinutes),
        );
        factory.register(
            "to_start_of_ten_minutes",
            Self::round_function_creator(Round::TenMinutes),
        );
        factory.register(
            "to_start_of_fifteen_minutes",
            Self::round_function_creator(Round::FifteenMinutes),
        );
        factory.register("time_slot", Self::round_function_creator(Round::TimeSlot));
        factory.register(
            "to_start_of_hour",
            Self::round_function_creator(Round::Hour),
        );
        factory.register("to_start_of_day", Self::round_function_creator(Round::Day));

        factory.register("to_start_of_week", ToStartOfWeekFunction::desc());

        // interval functions
        factory.register("add_years", AddYearsFunction::desc(1));
        factory.register("add_months", AddMonthsFunction::desc(1));
        factory.register("add_days", AddDaysFunction::desc(1));
        factory.register("add_hours", AddTimesFunction::desc(3600));
        factory.register("add_minutes", AddTimesFunction::desc(60));
        factory.register("add_seconds", AddTimesFunction::desc(1));
        factory.register("subtract_years", AddYearsFunction::desc(-1));
        factory.register("subtract_months", AddMonthsFunction::desc(-1));
        factory.register("subtract_days", AddDaysFunction::desc(-1));
        factory.register("subtract_hours", AddTimesFunction::desc(-3600));
        factory.register("subtract_minutes", AddTimesFunction::desc(-60));
        factory.register("subtract_seconds", AddTimesFunction::desc(-1));

        factory.register(
            "to_interval_year",
            to_interval_function_creator(IntervalKind::Year),
        );
        factory.register(
            "to_interval_month",
            to_interval_function_creator(IntervalKind::Month),
        );
        factory.register(
            "to_interval_day",
            to_interval_function_creator(IntervalKind::Day),
        );
        factory.register(
            "to_interval_hour",
            to_interval_function_creator(IntervalKind::Hour),
        );
        factory.register(
            "to_interval_minute",
            to_interval_function_creator(IntervalKind::Minute),
        );
        factory.register(
            "to_interval_second",
            to_interval_function_creator(IntervalKind::Second),
        );
        factory.register(
            "to_interval_doy",
            to_interval_function_creator(IntervalKind::Doy),
        );
        factory.register(
            "to_interval_dow",
            to_interval_function_creator(IntervalKind::Dow),
        );

        factory.register("date_add", DateAddFunction::desc());
    }
}
