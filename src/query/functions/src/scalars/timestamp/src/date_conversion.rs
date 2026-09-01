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

use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::TIMESTAMP_MAX;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_timezone::utc_from_local;
use jiff::SignedDuration;
use jiff::Unit;
use jiff::Zoned;
use jiff::civil::Date;
use jiff::civil::Time;
use jiff::civil::date;
use jiff::tz::TimeZone;

#[inline]
pub(super) fn today_date(now: &Zoned, tz: &TimeZone) -> i32 {
    let now = now.with_time_zone(tz.clone());
    now.date()
        .since((Unit::Day, Date::new(1970, 1, 1).unwrap()))
        .unwrap()
        .get_days()
}

// Summer Time in 1990 began at 2 a.m. (Beijing time) on Sunday, April 15th and ended at 2 a.m. (Beijing Daylight Saving Time) on Sunday, September 16th.
// During this period, the summer working hours will be implemented, namely from April 15th to September 16th.
// The working hours of all departments of The State Council are from 8 a.m. to 12 p.m. and from 1:30 p.m. to 5:30 p.m. The winter working hours will be implemented after September 17th.
pub fn calc_date_to_timestamp(val: i32, tz: &TimeZone) -> std::result::Result<i64, String> {
    let ts = (val as i64) * 24 * 3600 * MICROS_PER_SEC;
    let local_date = date_from_days(val);
    let year = i32::from(local_date.year());
    let month = local_date.month() as u8;
    let day = local_date.day() as u8;

    if let Some(micros) = utc_from_local(tz, year, month, day, 0, 0, 0, 0) {
        return ensure_timestamp_range(micros);
    }

    let midnight = local_date.to_datetime(Time::midnight());
    match midnight.to_zoned(tz.clone()) {
        Ok(zoned) => ensure_timestamp_range(zoned.timestamp().as_microsecond()),
        Err(_err) => {
            for minutes in 1..=1440 {
                let delta = SignedDuration::from_secs((minutes * 60) as i64);
                if let Ok(adj) = midnight.checked_add(delta) {
                    if let Ok(zoned) = adj.to_zoned(tz.clone()) {
                        return ensure_timestamp_range(zoned.timestamp().as_microsecond());
                    }
                } else {
                    break;
                }
            }

            // The timezone database might not have explicit rules for extremely
            // old/new dates, so fall back to the legacy behavior that applies the
            // canonical offset we use for 1970-01-01.
            let tz_offset_micros = tz
                .to_timestamp(date(1970, 1, 1).at(0, 0, 0, 0))
                .unwrap()
                .as_microsecond();
            ensure_timestamp_range(ts + tz_offset_micros)
        }
    }
}

fn ensure_timestamp_range(micros: i64) -> std::result::Result<i64, String> {
    if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&micros) {
        Ok(micros)
    } else {
        Err(format!(
            "Invalid date: timestamp value {micros} is out of range"
        ))
    }
}

fn normalize_time_precision(raw: i64) -> Result<u8, String> {
    if (0..=9).contains(&raw) {
        Ok(raw as u8)
    } else {
        Err(format!(
            "Invalid fractional seconds precision `{raw}` for `current_time` (expect 0-9)"
        ))
    }
}

fn current_time_string(func_ctx: &FunctionContext, precision: Option<u8>) -> String {
    let datetime = func_ctx.now.with_time_zone(func_ctx.tz.clone()).datetime();
    let nanos = datetime.subsec_nanosecond() as u32;
    let mut value = format!(
        "{:02}:{:02}:{:02}",
        datetime.hour(),
        datetime.minute(),
        datetime.second()
    );

    let precision = precision.unwrap_or(9).min(9);
    if precision > 0 {
        let divisor = 10_u32.pow(9 - precision as u32);
        let truncated = nanos / divisor;
        let frac = format!("{:0width$}", truncated, width = precision as usize);
        value.push('.');
        value.push_str(&frac);
    }

    value
}

pub(super) fn register_real_time_functions(registry: &mut FunctionRegistry) {
    registry.register_aliases("now", &["current_timestamp"]);
    registry.register_aliases("today", &["current_date"]);

    registry.properties.insert(
        "now".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "current_time".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "today".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "yesterday".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "tomorrow".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    // NOTE: `to_timestamp`/`to_timestamp_tz`/`to_date` keep their pre-existing global
    // monotonicity flags; they carry the same time-zone caveat as the calendar
    // projections and should eventually migrate to `monotonicity_check` too.
    for name in &["to_timestamp", "to_timestamp_tz", "to_date"] {
        registry
            .properties
            .insert(name.to_string(), FunctionProperty::default().monotonicity());
    }

    registry.properties.insert(
        "to_string".to_string(),
        FunctionProperty::default()
            .monotonicity_type(DataType::Timestamp)
            .monotonicity_type(DataType::Timestamp.wrap_nullable()),
    );

    registry.properties.insert(
        "to_string".to_string(),
        FunctionProperty::default()
            .monotonicity_type(DataType::Date)
            .monotonicity_type(DataType::Date.wrap_nullable()),
    );

    registry.register_0_arg_core::<TimestampType, _>(
        "now",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(ctx.func_ctx.now.timestamp().as_microsecond()),
    );

    registry.register_0_arg_core::<StringType, _>(
        "current_time",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(current_time_string(ctx.func_ctx, None)),
    );

    registry.register_passthrough_nullable_1_arg::<Int64Type, StringType, _>(
        "current_time",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<Int64Type, StringType>(|precision, output, ctx| {
            match normalize_time_precision(precision) {
                Ok(valid_precision) => {
                    output.put_and_commit(current_time_string(ctx.func_ctx, Some(valid_precision)));
                }
                Err(err) => {
                    ctx.set_error(output.len(), err);
                    output.commit_row();
                }
            }
        }),
    );

    registry.register_0_arg_core::<DateType, _>(
        "today",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz)),
    );

    registry.register_0_arg_core::<DateType, _>(
        "yesterday",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz) - 1),
    );

    registry.register_0_arg_core::<DateType, _>(
        "tomorrow",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(today_date(&ctx.func_ctx.now, &ctx.func_ctx.tz) + 1),
    );
}
