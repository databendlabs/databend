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

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DateType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::date::date_from_days;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::number::UInt16Type;
use databend_common_expression::types::number::UInt32Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::timestamp::MICROS_PER_SEC;
use databend_common_expression::types::timestamp::timestamp_from_micros;
use databend_common_expression::vectorize_1_arg;
use databend_common_timezone::DateTimeComponents;
use databend_common_timezone::fast_components_from_timestamp;
use jiff::Zoned;
use jiff::civil::Date;
use jiff::tz::TimeZone;

pub(super) trait ToNumber {
    type Output;

    fn to_number(dt: &Zoned) -> Self::Output;

    fn from_components(components: &DateTimeComponents) -> Self::Output;
}

trait DateToNumber: ToNumber {
    fn to_number_from_date(date: &Date) -> Self::Output;
}

struct ToNumberImpl;

impl ToNumberImpl {
    fn eval_timestamp<T: ToNumber>(us: i64, tz: &TimeZone) -> T::Output {
        if let Some(components) = fast_components_from_timestamp(us, tz) {
            return T::from_components(&components);
        }
        let dt = timestamp_from_micros(us, tz);
        T::to_number(&dt)
    }

    fn eval_date<T: DateToNumber>(days: i32) -> T::Output {
        T::to_number_from_date(&date_from_days(days as i64))
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

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.year() as u32 * 100 + dt.month() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.year as u32 * 100 + components.month as u32
    }
}

impl DateToNumber for ToYYYYMM {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.year() as u32 * 100 + date.month() as u32
    }
}

impl ToNumber for ToMillennium {
    type Output = u16;

    fn to_number(dt: &Zoned) -> Self::Output {
        (dt.year() as u16).div_ceil(1000)
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        (components.year as u16).div_ceil(1000)
    }
}

impl DateToNumber for ToMillennium {
    fn to_number_from_date(date: &Date) -> Self::Output {
        (date.year() as u16).div_ceil(1000)
    }
}

impl ToNumber for ToWeekOfYear {
    type Output = u32;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.date().iso_week_date().week() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.iso_year_week().1
    }
}

impl DateToNumber for ToWeekOfYear {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.iso_week_date().week() as u32
    }
}

impl ToNumber for ToYYYYMMDD {
    type Output = u32;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.year() as u32 * 10_000 + dt.month() as u32 * 100 + dt.day() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.year as u32 * 10_000 + components.month as u32 * 100 + components.day as u32
    }
}

impl DateToNumber for ToYYYYMMDD {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.year() as u32 * 10_000 + date.month() as u32 * 100 + date.day() as u32
    }
}

impl ToNumber for ToYYYYMMDDHH {
    type Output = u64;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.year() as u64 * 1_000_000
            + dt.month() as u64 * 10_000
            + dt.day() as u64 * 100
            + dt.hour() as u64
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.year as u64 * 1_000_000
            + components.month as u64 * 10_000
            + components.day as u64 * 100
            + components.hour as u64
    }
}

impl ToNumber for ToYYYYMMDDHHMMSS {
    type Output = u64;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.year() as u64 * 10_000_000_000
            + dt.month() as u64 * 100_000_000
            + dt.day() as u64 * 1_000_000
            + dt.hour() as u64 * 10_000
            + dt.minute() as u64 * 100
            + dt.second() as u64
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.year as u64 * 10_000_000_000
            + components.month as u64 * 100_000_000
            + components.day as u64 * 1_000_000
            + components.hour as u64 * 10_000
            + components.minute as u64 * 100
            + components.second as u64
    }
}

impl ToNumber for ToYear {
    type Output = u16;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.year() as u16
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.year as u16
    }
}

impl DateToNumber for ToYear {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.year() as u16
    }
}

impl ToNumber for ToISOYear {
    type Output = u16;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.date().iso_week_date().year() as _
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.iso_year_week().0 as u16
    }
}

impl DateToNumber for ToISOYear {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.iso_week_date().year() as u16
    }
}

impl ToNumber for ToYYYYWW {
    type Output = u32;

    fn to_number(dt: &Zoned) -> Self::Output {
        let week_date = dt.date().iso_week_date();
        let year = week_date.year() as u32 * 100;
        year + dt.date().iso_week_date().week() as u32
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        let (iso_year, iso_week) = components.iso_year_week();
        iso_year as u32 * 100 + iso_week
    }
}

impl DateToNumber for ToYYYYWW {
    fn to_number_from_date(date: &Date) -> Self::Output {
        let week_date = date.iso_week_date();
        week_date.year() as u32 * 100 + week_date.week() as u32
    }
}

impl ToNumber for ToQuarter {
    type Output = u8;

    fn to_number(dt: &Zoned) -> Self::Output {
        // begin with 0
        ((dt.month() - 1) / 3 + 1) as u8
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        (components.month - 1) / 3 + 1
    }
}

impl DateToNumber for ToQuarter {
    fn to_number_from_date(date: &Date) -> Self::Output {
        (date.month() as u8 - 1) / 3 + 1
    }
}

impl ToNumber for ToMonth {
    type Output = u8;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.month() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.month
    }
}

impl DateToNumber for ToMonth {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.month() as u8
    }
}

impl ToNumber for ToDayOfYear {
    type Output = u16;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.day_of_year() as u16
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.day_of_year
    }
}

impl DateToNumber for ToDayOfYear {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.day_of_year() as u16
    }
}

impl ToNumber for ToDayOfMonth {
    type Output = u8;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.day() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.day
    }
}

impl DateToNumber for ToDayOfMonth {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.day() as u8
    }
}

impl ToNumber for ToDayOfWeek {
    type Output = u8;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.weekday().to_monday_one_offset() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.weekday.to_monday_one_offset() as u8
    }
}

impl DateToNumber for ToDayOfWeek {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.weekday().to_monday_one_offset() as u8
    }
}

impl ToNumber for DayOfWeek {
    type Output = u8;

    fn to_number(dt: &Zoned) -> Self::Output {
        dt.weekday().to_sunday_zero_offset() as u8
    }

    fn from_components(components: &DateTimeComponents) -> Self::Output {
        components.weekday.to_sunday_zero_offset() as u8
    }
}

impl DateToNumber for DayOfWeek {
    fn to_number_from_date(date: &Date) -> Self::Output {
        date.weekday().to_sunday_zero_offset() as u8
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
    // date
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_yyyymm",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, _| ToNumberImpl::eval_date::<ToYYYYMM>(val)),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _>(
        "to_yyyymmdd",
        |_, _| FunctionDomain::Full,
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
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "to_yyyymm",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMM>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "to_yyyymmdd",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMMDD>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt64Type, _>(
        "to_yyyymmddhh",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMMDDHH>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt64Type, _>(
        "to_yyyymmddhhmmss",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYMMDDHHMMSS>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "to_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYear>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "to_iso_year",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToISOYear>(val, &ctx.func_ctx.tz),
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
    registry.register_1_arg::<TimestampType, UInt32Type, _>(
        "yearweek",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToYYYYWW>(val, &ctx.func_ctx.tz),
    );
    registry.register_1_arg::<TimestampType, UInt16Type, _>(
        "millennium",
        |_, _| FunctionDomain::Full,
        |val, ctx| ToNumberImpl::eval_timestamp::<ToMillennium>(val, &ctx.func_ctx.tz),
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
        |val, ctx| {
            let datetime = timestamp_from_micros(val, &ctx.func_ctx.tz);
            datetime.hour() as u8
        },
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_minute",
        |_, _| FunctionDomain::Full,
        |val, ctx| {
            let datetime = timestamp_from_micros(val, &ctx.func_ctx.tz);
            datetime.minute() as u8
        },
    );
    registry.register_1_arg::<TimestampType, UInt8Type, _>(
        "to_second",
        |_, _| FunctionDomain::Full,
        |val, ctx| {
            let datetime = timestamp_from_micros(val, &ctx.func_ctx.tz);
            datetime.second() as u8
        },
    );
}
