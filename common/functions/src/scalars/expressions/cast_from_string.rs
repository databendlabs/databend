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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::NaiveDateTime;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::cast_with_type::arrow_cast_compute;
use super::cast_with_type::new_mutable_bitmap;
use super::cast_with_type::CastOptions;

pub fn cast_from_string(
    column: &ColumnRef,
    data_type: &DataTypePtr,
    cast_options: &CastOptions,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let str_column = Series::remove_nullable(column);
    let str_column: &StringColumn = Series::check_get(&str_column)?;
    let size = str_column.len();
    let mut bitmap = new_mutable_bitmap(size, true);

    match data_type.data_type_id() {
        TypeID::Date16 => {
            let mut builder = ColumnBuilder::<u16>::with_capacity(size);

            for (row, v) in str_column.iter().enumerate() {
                if let Some(d) = string_to_date(v) {
                    builder.append((d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as u16);
                } else {
                    bitmap.set(row, false)
                }
            }

            Ok((builder.build(size), Some(bitmap.into())))
        }

        TypeID::Date32 => {
            let mut builder = ColumnBuilder::<i32>::with_capacity(size);

            for (row, v) in str_column.iter().enumerate() {
                match string_to_date(v) {
                    Some(d) => {
                        builder.append((d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i32);
                    }
                    None => bitmap.set(row, false),
                }
            }
            Ok((builder.build(size), Some(bitmap.into())))
        }

        TypeID::DateTime32 => {
            let mut builder = ColumnBuilder::<u32>::with_capacity(size);

            for (row, v) in str_column.iter().enumerate() {
                match string_to_datetime(v) {
                    Some(t) => {
                        builder.append(t.timestamp() as u32);
                    }
                    None => bitmap.set(row, false),
                }
            }
            Ok((builder.build(size), Some(bitmap.into())))
        }
        TypeID::DateTime64 => {
            let mut builder = ColumnBuilder::<i64>::with_capacity(size);
            for (row, v) in str_column.iter().enumerate() {
                match string_to_datetime64(v) {
                    Some(d) => {
                        builder.append(d.timestamp_nanos());
                    }
                    None => bitmap.set(row, false),
                }
            }
            Ok((builder.build(size), Some(bitmap.into())))
        }
        TypeID::Interval => todo!(),
        _ => arrow_cast_compute(column, data_type, cast_options),
    }
}

// currently use UTC by default
// TODO support timezone
#[inline]
fn string_to_datetime(date_str: impl AsRef<[u8]>) -> Option<NaiveDateTime> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    s.and_then(|c| NaiveDateTime::parse_from_str(c, "%Y-%m-%d %H:%M:%S").ok())
}

// TODO support timezone
#[inline]
fn string_to_datetime64(date_str: impl AsRef<[u8]>) -> Option<NaiveDateTime> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    s.and_then(|c| NaiveDateTime::parse_from_str(c, "%Y-%m-%d %H:%M:%S%.9f").ok())
}

#[inline]
fn string_to_date(date_str: impl AsRef<[u8]>) -> Option<NaiveDate> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    s.and_then(|c| c.parse::<NaiveDate>().ok())
}
