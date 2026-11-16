// Copyright 2021 Datafuse Labs
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

use chrono::Weekday;
use chrono_tz::Tz;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::DateType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_timezone::components_from_timestamp;
use databend_common_timezone::fast_utc_from_local;

use crate::date_arithmetic::ensure_date_range;
use crate::interval::civil_date_from_days;
use crate::interval::civil_date_to_days;
use crate::interval::ensure_timestamp_range;

// 1970-01-01 was Thursday.
const EPOCH_MONDAY_ZERO_OFFSET: i128 = 3;

const OUT_OF_RANGE: &str = "Invalid date: time_slice result is out of range";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimePart {
    Year,
    Quarter,
    Month,
    Week,
    IsoWeek,
    Day,
    Hour,
    Minute,
    Second,
}

impl TimePart {
    // Parsed for every row, so compared in place rather than uppercased into
    // a new string.
    fn parse(s: &str) -> Option<Self> {
        [
            ("YEAR", Self::Year),
            ("QUARTER", Self::Quarter),
            ("MONTH", Self::Month),
            ("WEEK", Self::Week),
            ("ISOWEEK", Self::IsoWeek),
            ("DAY", Self::Day),
            ("HOUR", Self::Hour),
            ("MINUTE", Self::Minute),
            ("SECOND", Self::Second),
        ]
        .into_iter()
        .find_map(|(name, part)| s.eq_ignore_ascii_case(name).then_some(part))
    }

    fn is_date_part(self) -> bool {
        matches!(
            self,
            Self::Year | Self::Quarter | Self::Month | Self::Week | Self::IsoWeek | Self::Day
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartOrEnd {
    Start,
    End,
}

impl StartOrEnd {
    fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("START") {
            Some(Self::Start)
        } else if s.eq_ignore_ascii_case("END") {
            Some(Self::End)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SliceSpec {
    slice_length: i128,
    part: TimePart,
    start_or_end: StartOrEnd,
    week_start: Weekday,
}

impl SliceSpec {
    fn new(
        slice_length: u64,
        part: &str,
        start_or_end: &str,
        week_start: u8,
        date_input: bool,
    ) -> Result<Self, String> {
        if slice_length < 1 {
            return Err("slice_length must be greater than or equal to 1".to_string());
        }
        let part = TimePart::parse(part).ok_or_else(|| {
            format!(
                "time_slice does not support `{part}`, expecting one of \
                 [year, quarter, month, week, isoweek, day, hour, minute, second]"
            )
        })?;
        if date_input && !part.is_date_part() {
            return Err("Date type only support Year | Quarter | Month | Week | Day".to_string());
        }
        let start_or_end = StartOrEnd::parse(start_or_end)
            .ok_or_else(|| "time_slice only support start or end".to_string())?;

        Ok(Self {
            slice_length: i128::from(slice_length),
            part,
            start_or_end,
            // Preserve the historical 0=Sunday, nonzero=Monday convention.
            week_start: if week_start == 0 {
                Weekday::Sun
            } else {
                Weekday::Mon
            },
        })
    }

    fn pick(self, start: i128, end: i128) -> i128 {
        match self.start_or_end {
            StartOrEnd::Start => start,
            StartOrEnd::End => end,
        }
    }
}

// Euclidean division keeps pre-epoch slices aligned.
fn floor_to_multiple(value: i128, slice: i128) -> i128 {
    value.div_euclid(slice) * slice
}

fn month_index_to_days(month_index: i128) -> Option<i128> {
    let year = i64::try_from(1970 + month_index.div_euclid(12)).ok()?;
    let month = (month_index.rem_euclid(12) + 1) as u8;
    Some(civil_date_to_days(year, month, 1))
}

// Widened intermediates keep slice boundaries range-checkable before SQL output.
fn slice_bounds_days(days: i128, spec: SliceSpec) -> Option<(i128, i128)> {
    match spec.part {
        TimePart::Year | TimePart::Quarter | TimePart::Month => {
            let unit_months: i128 = match spec.part {
                TimePart::Year => 12,
                TimePart::Quarter => 3,
                _ => 1,
            };
            let slice_months = spec.slice_length.checked_mul(unit_months)?;
            let (year, month, _) = civil_date_from_days(days);
            let months_since_epoch = (year - 1970) * 12 + (i128::from(month) - 1);
            let start = floor_to_multiple(months_since_epoch, slice_months);
            let end = start.checked_add(slice_months)?;
            Some((month_index_to_days(start)?, month_index_to_days(end)?))
        }
        TimePart::Week | TimePart::IsoWeek => {
            let week_start = if spec.part == TimePart::IsoWeek {
                Weekday::Mon
            } else {
                spec.week_start
            };
            let slice_days = spec.slice_length.checked_mul(7)?;
            let epoch_shift = (EPOCH_MONDAY_ZERO_OFFSET
                - i128::from(week_start.num_days_from_monday()))
            .rem_euclid(7);
            let start = floor_to_multiple(days.checked_add(epoch_shift)?, slice_days) - epoch_shift;
            Some((start, start.checked_add(slice_days)?))
        }
        TimePart::Day => {
            let start = floor_to_multiple(days, spec.slice_length);
            Some((start, start.checked_add(spec.slice_length)?))
        }
        TimePart::Hour | TimePart::Minute | TimePart::Second => None,
    }
}

// Sub-day slices stay fixed-width across timezone transitions.
fn slice_bounds_micros(micros: i64, spec: SliceSpec) -> Option<(i128, i128)> {
    let unit_seconds: i128 = match spec.part {
        TimePart::Hour => 3600,
        TimePart::Minute => 60,
        TimePart::Second => 1,
        _ => return None,
    };
    let slice_micros = spec
        .slice_length
        .checked_mul(unit_seconds)?
        .checked_mul(i128::from(MICROS_PER_SEC))?;
    // A slice that fits i64 is floored in i64: i128 division is a software
    // routine, and the dominant cost of this function per row. The bounds
    // stay i128, so an end past i64 is reported exactly as before.
    if let Ok(slice) = i64::try_from(slice_micros) {
        let start = i128::from(micros.div_euclid(slice)) * i128::from(slice);
        return Some((start, start + i128::from(slice)));
    }
    let start = floor_to_multiple(i128::from(micros), slice_micros);
    Some((start, start.checked_add(slice_micros)?))
}

fn time_slice_date(date: i32, spec: SliceSpec) -> Result<i32, String> {
    let (start, end) =
        slice_bounds_days(i128::from(date), spec).ok_or_else(|| OUT_OF_RANGE.to_string())?;
    let days = i64::try_from(spec.pick(start, end)).map_err(|_| OUT_OF_RANGE.to_string())?;
    ensure_date_range(days)
}

fn time_slice_timestamp(ts: i64, spec: SliceSpec, tz: &Tz) -> Result<i64, String> {
    let micros = if spec.part.is_date_part() {
        // Calendar slice boundaries are local midnights.
        let components = components_from_timestamp(ts, tz);
        let days = civil_date_to_days(i64::from(components.year), components.month, components.day);
        let (start, end) = slice_bounds_days(days, spec).ok_or_else(|| OUT_OF_RANGE.to_string())?;
        let (year, month, day) = civil_date_from_days(spec.pick(start, end));
        let year = i32::try_from(year).map_err(|_| OUT_OF_RANGE.to_string())?;
        fast_utc_from_local(tz, year, month, day, 0, 0, 0, 0)
            .ok_or_else(|| OUT_OF_RANGE.to_string())?
    } else {
        let (start, end) = slice_bounds_micros(ts, spec).ok_or_else(|| OUT_OF_RANGE.to_string())?;
        i64::try_from(spec.pick(start, end)).map_err(|_| OUT_OF_RANGE.to_string())?
    };
    ensure_timestamp_range(micros)
}

// The binder passes the slice as constants (lower_time_slice_expr), so its
// spec is built once for a column rather than once per row. A spec that does
// not parse leaves the column to the per-row path, which reports the error on
// every row as before.
fn constant_spec(
    slice_length: &Value<UInt64Type>,
    start_or_end: &Value<StringType>,
    part: &Value<StringType>,
    week_start: u8,
    date_input: bool,
) -> Option<SliceSpec> {
    let (Value::Scalar(slice_length), Value::Scalar(start_or_end), Value::Scalar(part)) =
        (slice_length, start_or_end, part)
    else {
        return None;
    };
    SliceSpec::new(*slice_length, part, start_or_end, week_start, date_input).ok()
}

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_4_arg::<DateType, UInt64Type, StringType, StringType, DateType, _, _>(
        "time_slice",
        |_, _, _, _, _| FunctionDomain::MayThrow,
        |date, slice_length, start_or_end, part, ctx| {
            if let Value::Column(dates) = &date
                && let Some(spec) =
                    constant_spec(&slice_length, &start_or_end, &part, ctx.func_ctx.week_start, true)
            {
                let mut output = Vec::with_capacity(dates.len());
                for (row, date) in dates.iter().enumerate() {
                    match time_slice_date(*date, spec) {
                        Ok(value) => output.push(value),
                        Err(error) => {
                            ctx.set_error(row, error);
                            output.push(0);
                        }
                    }
                }
                return Value::Column(output.into());
            }
            vectorize_with_builder_4_arg::<DateType, UInt64Type, StringType, StringType, DateType>(
                |date, slice_length, start_or_end, part, output, ctx| {
                    let result = SliceSpec::new(
                        slice_length,
                        part,
                        start_or_end,
                        ctx.func_ctx.week_start,
                        true,
                    )
                    .and_then(|spec| time_slice_date(date, spec));
                    match result {
                        Ok(value) => output.push(value),
                        Err(error) => {
                            ctx.set_error(output.len(), error);
                            output.push(0);
                        }
                    }
                },
            )(date, slice_length, start_or_end, part, ctx)
        },
    );
    registry.register_passthrough_nullable_4_arg::<TimestampType, UInt64Type, StringType, StringType, TimestampType, _, _>(
        "time_slice",
        |_, _, _, _, _| FunctionDomain::MayThrow,
        |ts, slice_length, start_or_end, part, ctx| {
            if let Value::Column(timestamps) = &ts
                && let Some(spec) =
                    constant_spec(&slice_length, &start_or_end, &part, ctx.func_ctx.week_start, false)
            {
                let tz = ctx.func_ctx.tz;
                let mut output = Vec::with_capacity(timestamps.len());
                for (row, ts) in timestamps.iter().enumerate() {
                    match time_slice_timestamp(*ts, spec, &tz) {
                        Ok(value) => output.push(value),
                        Err(error) => {
                            ctx.set_error(row, error);
                            output.push(0);
                        }
                    }
                }
                return Value::Column(output.into());
            }
            vectorize_with_builder_4_arg::<TimestampType, UInt64Type, StringType, StringType, TimestampType>(
                |ts, slice_length, start_or_end, part, output, ctx| {
                    let result = SliceSpec::new(
                        slice_length,
                        part,
                        start_or_end,
                        ctx.func_ctx.week_start,
                        false,
                    )
                    .and_then(|spec| time_slice_timestamp(ts, spec, &ctx.func_ctx.tz));
                    match result {
                        Ok(value) => output.push(value),
                        Err(error) => {
                            ctx.set_error(output.len(), error);
                            output.push(0);
                        }
                    }
                },
            )(ts, slice_length, start_or_end, part, ctx)
        },
    );
}
