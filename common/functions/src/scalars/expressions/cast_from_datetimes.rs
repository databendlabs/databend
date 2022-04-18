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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::cast_with_type::arrow_cast_compute;
use super::cast_with_type::CastOptions;

const DATE_FMT: &str = "%Y-%m-%d";
const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

pub fn cast_from_date(
    column: &ColumnRef,
    from_type: &DataTypePtr,
    data_type: &DataTypePtr,
    cast_options: &CastOptions,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let c = Series::remove_nullable(column);
    let c: &Int32Column = Series::check_get(&c)?;
    let size = c.len();

    match data_type.data_type_id() {
        TypeID::String => {
            let mut builder = ColumnBuilder::<Vu8>::with_capacity(size);

            for v in c.iter() {
                let s = datetime_to_string(Utc.timestamp(*v as i64 * 24 * 3600, 0_u32), DATE_FMT);
                builder.append(s.as_bytes());
            }
            Ok((builder.build(size), None))
        }

        TypeID::DateTime => {
            let datetime = data_type.as_any().downcast_ref::<DateTimeType>().unwrap();
            let it = c
                .iter()
                .map(|v| datetime.from_nano_seconds(*v as i64 * 24 * 3600 * 1_000_000_000));
            let result = Arc::new(Int64Column::from_iterator(it));
            Ok((result, None))
        }

        _ => arrow_cast_compute(column, from_type, data_type, cast_options),
    }
}

pub fn cast_from_datetime(
    column: &ColumnRef,
    from_type: &DataTypePtr,
    data_type: &DataTypePtr,
    cast_options: &CastOptions,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let c = Series::remove_nullable(column);
    let c: &Int64Column = Series::check_get(&c)?;
    let size = c.len();

    let date_time64 = from_type.as_any().downcast_ref::<DateTimeType>().unwrap();

    match data_type.data_type_id() {
        TypeID::String => {
            let mut builder = MutableStringColumn::with_capacity(size);
            for v in c.iter() {
                let s = datetime_to_string(
                    date_time64.utc_timestamp(*v),
                    date_time64.format_string().as_str(),
                );
                builder.append_value(s.as_bytes());
            }
            Ok((builder.to_column(), None))
        }

        TypeID::Date => {
            let it = c
                .iter()
                .map(|v| (date_time64.to_seconds(*v) / 24 / 3600) as i32);
            let result = Arc::new(Int32Column::from_iterator(it));
            Ok((result, None))
        }

        TypeID::DateTime => {
            let it = c.iter().map(|v| date_time64.to_seconds(*v) as i64);
            let result = Arc::new(Int64Column::from_iterator(it));
            Ok((result, None))
        }

        _ => arrow_cast_compute(column, from_type, data_type, cast_options),
    }
}

#[inline]
fn datetime_to_string(date: DateTime<Utc>, fmt: &str) -> String {
    date.format(fmt).to_string()
}
