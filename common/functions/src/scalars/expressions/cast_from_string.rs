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

use chrono_tz::Tz;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::TimeZone;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::cast_with_type::arrow_cast_compute;
use super::cast_with_type::CastOptions;
use crate::scalars::FunctionContext;

const DATE_TIME_LEN: usize = 19;

pub fn cast_from_string(
    column: &ColumnRef,
    from_type: &DataTypeImpl,
    data_type: &DataTypeImpl,
    cast_options: &CastOptions,
    func_ctx: &FunctionContext,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let str_column = Series::remove_nullable(column);
    let str_column: &StringColumn = Series::check_get(&str_column)?;
    let size = str_column.len();

    match data_type.data_type_id() {
        TypeID::Date => {
            let mut builder = NullableColumnBuilder::<i32>::with_capacity(size);

            for v in str_column.iter() {
                if let Some(d) = string_to_date(v) {
                    builder.append((d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i32, true);
                } else {
                    builder.append_null();
                }
            }
            let column = builder.build(size);
            let nullable_column: &NullableColumn = Series::check_get(&column)?;
            Ok((
                nullable_column.inner().clone(),
                Some(nullable_column.ensure_validity().clone()),
            ))
        }

        TypeID::Timestamp => {
            let mut builder = NullableColumnBuilder::<i64>::with_capacity(size);
            let tz = func_ctx.tz;
            for v in str_column.iter() {
                match string_to_timestamp(v, &tz) {
                    Some(d) => builder.append(d.timestamp_micros(), true),
                    None => builder.append_null(),
                }
            }
            let column = builder.build(size);
            let nullable_column: &NullableColumn = Series::check_get(&column)?;
            Ok((
                nullable_column.inner().clone(),
                Some(nullable_column.ensure_validity().clone()),
            ))
        }
        TypeID::Boolean => {
            let mut builder = NullableColumnBuilder::<bool>::with_capacity(size);
            for v in str_column.iter() {
                if v.eq_ignore_ascii_case("true".as_bytes()) {
                    builder.append(true, true);
                } else if v.eq_ignore_ascii_case("false".as_bytes()) {
                    builder.append(false, true);
                } else {
                    builder.append_null();
                }
            }
            let column = builder.build(size);
            let nullable_column: &NullableColumn = Series::check_get(&column)?;
            Ok((
                nullable_column.inner().clone(),
                Some(nullable_column.ensure_validity().clone()),
            ))
        }
        TypeID::Interval => todo!(),
        _ => arrow_cast_compute(column, from_type, data_type, cast_options, func_ctx),
    }
}

// TODO support timezone
#[inline]
pub fn string_to_timestamp(date_str: impl AsRef<[u8]>, tz: &Tz) -> Option<DateTime<Tz>> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    if let Some(s) = s {
        // convert zero timestamp to `1970-01-01 00:00:00`
        if s.len() >= DATE_TIME_LEN
            && (&s[..DATE_TIME_LEN] == "0000-00-00 00:00:00"
                || &s[..DATE_TIME_LEN] == "0000-00-00T00:00:00")
        {
            let t = format!("1970-01-01 00:00:00{}", &s[DATE_TIME_LEN..]);
            tz.datetime_from_str(&t, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| tz.datetime_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%z"))
                .ok()
        } else {
            match tz
                .datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| tz.datetime_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
            {
                Ok(dt) => {
                    // convert timestamp less than `1000-01-01 00:00:00` to `1000-01-01 00:00:00`
                    if dt.year() < 1000 {
                        Some(
                            tz.from_utc_datetime(&NaiveDate::from_ymd(1000, 1, 1).and_hms(0, 0, 0)),
                        )
                    } else {
                        Some(dt)
                    }
                }
                Err(_) => {
                    // DateTime::parse_from_str requires that it successfully parse an entire DateTime -- full date and time and timezone.
                    match DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%z") {
                        Ok(dt) => {
                            // convert timestamp less than `1000-01-01 00:00:00` to `1000-01-01 00:00:00`
                            if dt.year() < 1000 {
                                Some(tz.from_utc_datetime(
                                    &NaiveDate::from_ymd(1000, 1, 1).and_hms(0, 0, 0),
                                ))
                            } else {
                                Some(dt.with_timezone(tz))
                            }
                        }
                        Err(_) => None,
                    }
                }
            }
        }
    } else {
        None
    }
}

#[inline]
pub fn string_to_date(date_str: impl AsRef<[u8]>) -> Option<NaiveDate> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    if let Some(s) = s {
        // convert zero date to `1970-01-01`
        if s == "0000-00-00" {
            Some(NaiveDate::from_ymd(1970, 1, 1))
        } else {
            match s.parse::<NaiveDate>() {
                Ok(d) => {
                    // convert date less than `1000-01-01` to `1000-01-01`
                    if d.year() < 1000 {
                        Some(NaiveDate::from_ymd(1000, 1, 1))
                    } else {
                        Some(d)
                    }
                }
                Err(_) => None,
            }
        }
    } else {
        None
    }
}
