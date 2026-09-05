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

use chrono::DateTime;
use chrono::Datelike;
use chrono::Timelike;
use chrono::Utc;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::DateType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::utils::serialize::uniform_date;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_timezone::Tz;
use databend_common_timezone::fast_utc_from_local;
use databend_common_timezone::local_datetime_at;

#[inline]
pub(super) fn today_date(now: &DateTime<Utc>, tz: &Tz) -> i32 {
    let Some((local, _)) = local_datetime_at(tz, now.timestamp()) else {
        return 0;
    };
    uniform_date(local.date())
}

/// Midnight of a calendar date, as microseconds since the epoch.
///
/// Midnight does not exist on every date: `Asia/Shanghai` skipped
/// 1947-04-15 00:00:00 when DST began. Those cases follow the shared timezone
/// policy and move forward to the first instant that does exist.
pub fn calc_date_to_timestamp(val: i32, tz: &Tz) -> std::result::Result<i64, String> {
    let local_date = date_from_days(val);
    let year = local_date.year();
    let month = local_date.month() as u8;
    let day = local_date.day() as u8;

    let timestamp = fast_utc_from_local(tz, year, month, day, 0, 0, 0, 0).ok_or_else(|| {
        format!("Failed to convert date {local_date} to a timestamp in timezone {tz}")
    })?;
    check_timestamp(timestamp)
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
    let now = func_ctx.now;
    let Some((local, _)) = local_datetime_at(&func_ctx.tz, now.timestamp()) else {
        return "00:00:00".to_string();
    };
    let nanos = now.timestamp_subsec_nanos();

    let mut value = format!(
        "{:02}:{:02}:{:02}",
        local.hour(),
        local.minute(),
        local.second()
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

    // Conversion domains are calculated by their overloads. A global monotonic
    // flag is unsound for extended-year strings, AUTO numeric units, timezone
    // transitions, and conversions which can fail at the SQL range boundaries.
    // In particular, byte ordering of "+10000" and "9999" is reversed.

    registry.register_0_arg_core::<TimestampType, _>(
        "now",
        |_| FunctionDomain::Full,
        |ctx| Value::Scalar(ctx.func_ctx.now.timestamp_micros()),
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
