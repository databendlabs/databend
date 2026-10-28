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

use chrono::Datelike;
use chrono::NaiveDate;
use chrono_tz::Tz;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DateType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::DATE_MIN;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::number::UInt16Type;
use databend_common_expression::types::number::UInt32Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::TIMESTAMP_MIN;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_timezone::DateTimeComponents;
use databend_common_timezone::components_from_timestamp;
use databend_common_timezone::wall_clock_is_monotonic;

use crate::fixed_offset::local_dates;

/// Projection of a timestamp onto a calendar number, such as `to_year`.
pub(super) trait ToNumber {
    type Output;

    fn from_components(components: &DateTimeComponents) -> Self::Output;

    /// Year encoded by an unsigned projection, if any. Calendar and ISO years
    /// can be negative at the expanded TIMESTAMP boundary.
    fn unsigned_year(_components: &DateTimeComponents) -> Option<i32> {
        None
    }
}

/// Projections that also apply to a bare date, with no timezone involved.
trait DateToNumber: ToNumber {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output;
}

struct ToNumberImpl;

impl ToNumberImpl {
    fn eval_timestamp<T: ToNumber>(us: i64, tz: &Tz) -> T::Output {
        T::from_components(&components_from_timestamp(us, tz))
    }

    fn try_eval_timestamp<T: ToNumber>(us: i64, tz: &Tz) -> Result<T::Output, String> {
        let components = components_from_timestamp(us, tz);
        if T::unsigned_year(&components).is_some_and(|year| year < 0) {
            return Err("Invalid date: negative year cannot be represented by an unsigned calendar projection".to_string());
        }
        Ok(T::from_components(&components))
    }

    fn eval_date<T: DateToNumber>(days: i32) -> T::Output {
        T::to_number_from_date(&date_from_days(days as i64))
    }
}

fn eval_timestamp_projection<T: ToNumber<Output = N>, N: Number>(
    val: Value<TimestampType>,
    ctx: &mut EvalContext,
) -> Value<NumberType<N>> {
    vectorize_with_builder_1_arg::<TimestampType, NumberType<N>>(|val, output, ctx| {
        match ToNumberImpl::try_eval_timestamp::<T>(val, &ctx.func_ctx.tz) {
            Ok(value) => output.push(value),
            Err(err) => {
                ctx.set_error(output.len(), err);
                output.push(N::default());
            }
        }
    })(val, ctx)
}

/// Date -> calendar-number projection is unconditionally safe for end-point domain
/// evaluation: epoch days map to a proleptic Gregorian date without consulting the
/// session time zone.
fn date_to_u32_domain<T: DateToNumber<Output = u32>>(
    _ctx: &FunctionContext,
    domain: &SimpleDomain<i32>,
) -> FunctionDomain<UInt32Type> {
    FunctionDomain::Domain(SimpleDomain {
        min: ToNumberImpl::eval_date::<T>(domain.min),
        max: ToNumberImpl::eval_date::<T>(domain.max),
    })
}

/// End-point evaluation is sound only while the local wall clock is monotonic over the
/// full input range. A time-zone fallback may otherwise expose values outside the two
/// projected endpoints.
fn timestamp_to_u32_domain<T: ToNumber<Output = u32>>(
    ctx: &FunctionContext,
    domain: &SimpleDomain<i64>,
) -> FunctionDomain<UInt32Type> {
    let (Ok(min), Ok(max)) = (
        ToNumberImpl::try_eval_timestamp::<T>(domain.min, &ctx.tz),
        ToNumberImpl::try_eval_timestamp::<T>(domain.max, &ctx.tz),
    ) else {
        return FunctionDomain::MayThrow;
    };
    if !tz_wall_clock_single_segment(&ctx.tz, domain.min, domain.max) {
        // Outside the first UTC day of year 0, no supported timezone can
        // project into a negative year. Preserve Full for ordinary DST ranges;
        // only boundary ranges may throw even when both endpoints are valid.
        return if domain.min >= TIMESTAMP_MIN + 86_400 * MICROS_PER_SEC {
            FunctionDomain::Full
        } else {
            FunctionDomain::MayThrow
        };
    }
    FunctionDomain::Domain(SimpleDomain { min, max })
}

/// Whether `[min_us, max_us]` contains no time-zone transition. Within one offset
/// segment, the local wall clock is strictly increasing.
///
/// A transition landing exactly on `max_us` counts as inside the range, so the
/// answer stays conservative at the boundary.
pub(super) fn tz_wall_clock_single_segment(tz: &Tz, min_us: i64, max_us: i64) -> bool {
    wall_clock_is_monotonic(tz, min_us, max_us)
}

/// Range-sensitive monotonicity for calendar projections. Nullable domains are judged
/// by their non-null range; NULLs map to NULL and carry no ordering.
pub(super) fn calendar_monotonicity(ctx: &FunctionContext, args: &[Domain]) -> Option<usize> {
    let [domain] = args else {
        return None;
    };
    calendar_domain_monotonic(ctx, domain).then_some(0)
}

fn calendar_domain_monotonic(ctx: &FunctionContext, domain: &Domain) -> bool {
    match domain {
        Domain::Date(_) => true,
        Domain::Timestamp(simple) => tz_wall_clock_single_segment(&ctx.tz, simple.min, simple.max),
        Domain::Nullable(nullable) => match &nullable.value {
            Some(inner) => calendar_domain_monotonic(ctx, inner),
            // An all-NULL input projects to a single NULL output.
            None => true,
        },
        _ => false,
    }
}

struct ToYYYYMM;
struct ToYYYYWW;
struct ToYYYYMMDD;
struct ToYYYYMMDDHH;
struct ToYYYYMMDDHHMMSS;
struct ToYear;
struct ToMillennium;
struct ToISOYear;
pub(super) struct ToQuarter;
struct ToMonth;
struct ToDayOfYear;
struct ToDayOfMonth;
struct ToDayOfWeek;
struct DayOfWeek;

struct ToWeekOfYear;

impl ToNumber for ToYYYYMM {
    type Output = u32;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.year)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.year as u32 * 100 + c.month as u32
    }
}

impl DateToNumber for ToYYYYMM {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.year() as u32 * 100 + date.month()
    }
}

impl ToNumber for ToMillennium {
    type Output = u16;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.year)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        (c.year as u16).div_ceil(1000)
    }
}

impl DateToNumber for ToMillennium {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        (date.year() as u16).div_ceil(1000)
    }
}

impl ToNumber for ToWeekOfYear {
    type Output = u32;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.iso_year_week().1
    }
}

impl DateToNumber for ToWeekOfYear {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.iso_week().week()
    }
}

impl ToNumber for ToYYYYMMDD {
    type Output = u32;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.year)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.year as u32 * 10_000 + c.month as u32 * 100 + c.day as u32
    }
}

impl DateToNumber for ToYYYYMMDD {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.year() as u32 * 10_000 + date.month() * 100 + date.day()
    }
}

impl ToNumber for ToYYYYMMDDHH {
    type Output = u64;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.year)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.year as u64 * 1_000_000 + c.month as u64 * 10_000 + c.day as u64 * 100 + c.hour as u64
    }
}

impl ToNumber for ToYYYYMMDDHHMMSS {
    type Output = u64;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.year)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.year as u64 * 10_000_000_000
            + c.month as u64 * 100_000_000
            + c.day as u64 * 1_000_000
            + c.hour as u64 * 10_000
            + c.minute as u64 * 100
            + c.second as u64
    }
}

impl ToNumber for ToYear {
    type Output = u16;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.year)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.year as u16
    }
}

impl DateToNumber for ToYear {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.year() as u16
    }
}

impl ToNumber for ToISOYear {
    type Output = u16;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.iso_year_week().0)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.iso_year_week().0 as u16
    }
}

impl DateToNumber for ToISOYear {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.iso_week().year() as u16
    }
}

impl ToNumber for ToYYYYWW {
    type Output = u32;

    fn unsigned_year(c: &DateTimeComponents) -> Option<i32> {
        Some(c.iso_year_week().0)
    }

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        let (iso_year, iso_week) = c.iso_year_week();
        iso_year as u32 * 100 + iso_week
    }
}

impl DateToNumber for ToYYYYWW {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        let week = date.iso_week();
        week.year() as u32 * 100 + week.week()
    }
}

impl ToNumber for ToQuarter {
    type Output = u8;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        (c.month - 1) / 3 + 1
    }
}

impl DateToNumber for ToQuarter {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        (date.month() as u8 - 1) / 3 + 1
    }
}

impl ToNumber for ToMonth {
    type Output = u8;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.month
    }
}

impl DateToNumber for ToMonth {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.month() as u8
    }
}

impl ToNumber for ToDayOfYear {
    type Output = u16;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.day_of_year
    }
}

impl DateToNumber for ToDayOfYear {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.ordinal() as u16
    }
}

impl ToNumber for ToDayOfMonth {
    type Output = u8;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.day
    }
}

impl DateToNumber for ToDayOfMonth {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.day() as u8
    }
}

impl ToNumber for ToDayOfWeek {
    type Output = u8;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.weekday_from_monday_one()
    }
}

impl DateToNumber for ToDayOfWeek {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.weekday().number_from_monday() as u8
    }
}

impl ToNumber for DayOfWeek {
    type Output = u8;

    fn from_components(c: &DateTimeComponents) -> Self::Output {
        c.weekday_from_sunday_zero()
    }
}

impl DateToNumber for DayOfWeek {
    fn to_number_from_date(date: &NaiveDate) -> Self::Output {
        date.weekday().num_days_from_sunday() as u8
    }
}

pub(super) fn register_cast(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<DateType, NumberType<i64>, _>(
        "to_int64",
        |_, domain| FunctionDomain::Domain(domain.overflow_cast().0),
        |val, _| val as i64,
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, NumberType<i64>, _>(
        "to_int64",
        |_, domain| FunctionDomain::Domain(*domain),
        |val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(scalar),
            Value::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<DateType, NumberType<i64>, _, _>(
        "try_to_int64",
        |_, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.overflow_cast().0)),
            })
        },
        |val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(Some(scalar as i64)),
            Value::Column(col) => Value::Column(NullableColumn::new_unchecked(
                col.iter().map(|val| *val as i64).collect(),
                Bitmap::new_constant(true, col.len()),
            )),
        },
    );

    registry.register_combine_nullable_1_arg::<TimestampType, NumberType<i64>, _, _>(
        "try_to_int64",
        |_, domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(*domain)),
            })
        },
        |val, _| match val {
            Value::Scalar(scalar) => Value::Scalar(Some(scalar)),
            Value::Column(col) => {
                let validity = Bitmap::new_constant(true, col.len());
                Value::Column(NullableColumn::new_unchecked(col, validity))
            }
        },
    );
}

pub(super) fn register(registry: &mut FunctionRegistry) {
    // Calendar projections are monotonic only while the session time-zone wall clock is
    // monotonic over the input range.
    for name in ["to_yyyymm", "to_yyyymmdd"] {
        registry.properties.insert(
            name.to_string(),
            FunctionProperty::default().monotonicity_check(calendar_monotonicity),
        );
    }

    // date
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_yyyymm",
        date_to_u32_domain::<ToYYYYMM>,
        vectorize_1_arg::<DateType, UInt32Type>(|val, _| ToNumberImpl::eval_date::<ToYYYYMM>(val)),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_yyyymmdd",
        date_to_u32_domain::<ToYYYYMMDD>,
        vectorize_1_arg::<DateType, UInt32Type>(|val, _| {
            ToNumberImpl::eval_date::<ToYYYYMMDD>(val)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "to_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, _| ToNumberImpl::eval_date::<ToYear>(val)),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "to_iso_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, _| ToNumberImpl::eval_date::<ToISOYear>(val)),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_quarter",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, _| ToNumberImpl::eval_date::<ToQuarter>(val)),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, _| ToNumberImpl::eval_date::<ToMonth>(val)),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "to_day_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, _| {
            ToNumberImpl::eval_date::<ToDayOfYear>(val)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, _| {
            ToNumberImpl::eval_date::<ToDayOfMonth>(val)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "to_day_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, _| {
            ToNumberImpl::eval_date::<ToDayOfWeek>(val)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _>(
        "dayofweek",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, _| ToNumberImpl::eval_date::<DayOfWeek>(val)),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "yearweek",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, _| ToNumberImpl::eval_date::<ToYYYYWW>(val)),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _>(
        "millennium",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, _| {
            ToNumberImpl::eval_date::<ToMillennium>(val)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_week_of_year",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, _| {
            ToNumberImpl::eval_date::<ToWeekOfYear>(val)
        }),
    );
    // timestamp
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _>(
        "to_yyyymm",
        timestamp_to_u32_domain::<ToYYYYMM>,
        eval_timestamp_projection::<ToYYYYMM, u32>,
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _>(
        "to_yyyymmdd",
        timestamp_to_u32_domain::<ToYYYYMMDD>,
        |val, ctx| {
            if let Value::Column(col) = &val
                && let Some(days) = local_dates(col, ctx.validity.as_ref(), &ctx.func_ctx.tz)
                // Year 0 is a leap year. The date fast path casts years to u32,
                // so negative local years must use checked per-row evaluation.
                && days.iter().all(|&day| day >= DATE_MIN - 366)
            {
                return Value::Column(
                    days.iter()
                        .map(|&day| ToNumberImpl::eval_date::<ToYYYYMMDD>(day))
                        .collect(),
                );
            }
            eval_timestamp_projection::<ToYYYYMMDD, u32>(val, ctx)
        },
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt64Type, _>(
        "to_yyyymmddhh",
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_projection::<ToYYYYMMDDHH, u64>,
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt64Type, _>(
        "to_yyyymmddhhmmss",
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_projection::<ToYYYYMMDDHHMMSS, u64>,
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _>(
        "to_year",
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_projection::<ToYear, u16>,
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _>(
        "to_iso_year",
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_projection::<ToISOYear, u16>,
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_quarter",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToQuarter>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_month",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToMonth>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "to_day_of_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToDayOfYear>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_day_of_month",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToDayOfMonth>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_day_of_week",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToDayOfWeek>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "dayofweek",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<DayOfWeek>(val, &ctx.func_ctx.tz),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _>(
        "yearweek",
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_projection::<ToYYYYWW, u32>,
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _>(
        "millennium",
        |_, _| FunctionDomain::MayThrow,
        eval_timestamp_projection::<ToMillennium, u16>,
    );
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "to_week_of_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToWeekOfYear>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, Int64Type, _>(
        "to_unix_timestamp",
        |_, _| FunctionDomain::Full,
        |val, _| val.div_euclid(MICROS_PER_SEC),
    );

    registry.register_1_arg::<TimestampType, Float64Type, _>(
        "epoch",
        |_, domain| {
            FunctionDomain::Domain(SimpleDomain::<F64> {
                min: (domain.min as f64 / 1_000_000f64).into(),
                max: (domain.max as f64 / 1_000_000f64).into(),
            })
        },
        |val, _| (val as f64 / 1_000_000f64).into(),
    );

    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_hour",
        |_, _| FunctionDomain::Full,
        |val, ctx| components_from_timestamp(val, &ctx.func_ctx.tz).hour,
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_minute",
        |_, _| FunctionDomain::Full,
        |val, ctx| components_from_timestamp(val, &ctx.func_ctx.tz).minute,
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_second",
        |_, _| FunctionDomain::Full,
        |val, ctx| components_from_timestamp(val, &ctx.func_ctx.tz).second,
    );
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::timestamp::TIMESTAMP_MAX;

    use super::*;

    #[test]
    fn test_extended_timestamp_year_projections() {
        assert_eq!(
            ToNumberImpl::try_eval_timestamp::<ToYear>(TIMESTAMP_MIN, &Tz::UTC),
            Ok(0)
        );
        assert_eq!(
            ToNumberImpl::try_eval_timestamp::<ToYYYYMMDD>(TIMESTAMP_MIN, &Tz::UTC),
            Ok(101)
        );
        assert_eq!(
            ToNumberImpl::try_eval_timestamp::<ToYear>(TIMESTAMP_MAX, &Tz::UTC),
            Ok(10000)
        );
        // 0000-01-01 belongs to ISO year -1, even in UTC.
        assert!(ToNumberImpl::try_eval_timestamp::<ToISOYear>(TIMESTAMP_MIN, &Tz::UTC).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToYYYYWW>(TIMESTAMP_MIN, &Tz::UTC).is_err());

        let tz = "Etc/GMT+8".parse().unwrap();
        assert!(ToNumberImpl::try_eval_timestamp::<ToYear>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToISOYear>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToMillennium>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToYYYYMM>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToYYYYMMDD>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToYYYYMMDDHH>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToYYYYMMDDHHMMSS>(TIMESTAMP_MIN, &tz).is_err());
        assert!(ToNumberImpl::try_eval_timestamp::<ToYYYYWW>(TIMESTAMP_MIN, &tz).is_err());
        // Projections without a year remain valid in the preceding local year.
        assert_eq!(
            ToNumberImpl::try_eval_timestamp::<ToMonth>(TIMESTAMP_MIN, &tz),
            Ok(12)
        );
        assert_eq!(
            ToNumberImpl::try_eval_timestamp::<ToDayOfMonth>(TIMESTAMP_MIN, &tz),
            Ok(31)
        );
    }

    #[test]
    fn test_negative_year_projection_domain_may_throw() {
        let ctx = FunctionContext {
            tz: "Etc/GMT+8".parse().unwrap(),
            ..Default::default()
        };
        // Valid interior values must not hide the overflowing first row.
        let domain = SimpleDomain {
            min: TIMESTAMP_MIN,
            max: 0,
        };
        assert!(matches!(
            timestamp_to_u32_domain::<ToYYYYMM>(&ctx, &domain),
            FunctionDomain::MayThrow
        ));
        assert!(matches!(
            timestamp_to_u32_domain::<ToYYYYMMDD>(&ctx, &domain),
            FunctionDomain::MayThrow
        ));
    }

    #[test]
    fn test_date_projection_endpoints_unchanged() {
        assert_eq!(ToNumberImpl::eval_date::<ToYear>(DATE_MIN), 1);
        assert_eq!(ToNumberImpl::eval_date::<ToISOYear>(DATE_MIN), 1);
        assert_eq!(ToNumberImpl::eval_date::<ToYYYYMMDD>(DATE_MIN), 10101);
        let max = databend_common_expression::types::date::DATE_MAX;
        assert_eq!(ToNumberImpl::eval_date::<ToYear>(max), 9999);
        assert_eq!(ToNumberImpl::eval_date::<ToYYYYMMDD>(max), 99991231);
    }
}
