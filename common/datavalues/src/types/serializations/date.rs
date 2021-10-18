// Copyright 2020 Datafuse Labs.
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

use std::ops::AddAssign;
use std::ops::Sub;

use chrono::Date;
use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::TimeZone;
use chrono_tz::Tz;
use common_exception::*;
use common_io::prelude::*;
use lexical_core::FromLexical;
use num::cast::AsPrimitive;

use crate::prelude::*;

pub struct DateSerializer<T: DFPrimitiveType> {
    pub builder: PrimitiveArrayBuilder<T>,
}

impl<T> TypeSerializer for DateSerializer<T>
where
    i64: AsPrimitive<T>,
    T: DFPrimitiveType,
    T: Unmarshal<T> + StatBuffer + FromLexical,
    DFPrimitiveArray<T>: IntoSeries,
{
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let array = column.to_array()?;
        let array: &DFPrimitiveArray<T> = array.static_cast();

        let result: Vec<String> = array
            .iter()
            .map(|x| {
                x.map(|v| {
                    let mut date = NaiveDate::from_ymd(1970, 1, 1);
                    let d = Duration::days(v.to_i64().unwrap());
                    date.add_assign(d);
                    date.format("%Y-%m-%d").to_string()
                })
                .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        Ok(result)
    }

    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: T = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T = reader.read_scalar()?;
            self.builder.append_value(value);
        }
        Ok(())
    }

    fn de_text(&mut self, reader: &[u8]) -> Result<()> {
        if reader.eq_ignore_ascii_case(b"null") {
            self.builder.append_null();
            return Ok(());
        }

        match lexical_core::parse::<T>(reader) {
            Ok(v) => {
                self.builder.append_value(v);
                Ok(())
            }
            Err(_) => {
                let v = std::str::from_utf8(reader)
                    .map_err_to_code(ErrorCode::BadBytes, || "Cannot convert value to utf8")?;
                let res = v
                    .parse::<chrono::NaiveDate>()
                    .map_err_to_code(ErrorCode::BadBytes, || "Cannot parse value to Date type")?;
                let epoch = NaiveDate::from_ymd(1970, 1, 1);
                let duration = res.sub(epoch);
                self.builder.append_value(duration.num_days().as_());
                Ok(())
            }
        }
    }

    fn de_null(&mut self) {
        self.builder.append_null()
    }

    fn finish_to_series(&mut self) -> Series {
        self.builder.finish().into_series()
    }
}

pub trait DateConverter {
    fn to_date(&self, tz: &Tz) -> Date<Tz>;
    fn to_date_time(&self, tz: &Tz) -> DateTime<Tz>;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, tz: &Tz) -> Date<Tz> {
        let mut dt = tz.ymd(1970, 1, 1);
        dt = dt.checked_add_signed(Duration::days(self.as_())).unwrap();
        dt
    }

    fn to_date_time(&self, tz: &Tz) -> DateTime<Tz> {
        tz.timestamp_millis(self.as_() * 1000)
    }
}
