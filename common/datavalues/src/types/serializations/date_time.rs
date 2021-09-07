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

use std::marker::PhantomData;
use std::ops::AddAssign;

use chrono::Duration;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono_tz::Tz;
use common_exception::*;
use common_io::prelude::*;
use lexical_core::FromLexical;
use num::cast::AsPrimitive;

use crate::prelude::*;

pub struct DateTimeSerializer<T: DFPrimitiveType> {
    t: PhantomData<T>,
}

impl<T: DFPrimitiveType> Default for DateTimeSerializer<T> {
    fn default() -> Self {
        Self {
            t: Default::default(),
        }
    }
}

impl<T: DFPrimitiveType> TypeSerializer for DateTimeSerializer<T> {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let array = column.to_array()?;
        let array: &DFPrimitiveArray<T> = array.static_cast();

        let result: Vec<String> = array
            .iter()
            .map(|x| {
                x.map(|v| {
                    let mut dt = NaiveDateTime::from_timestamp(0, 0);
                    let d = Duration::seconds(v.to_i64().unwrap());
                    dt.add_assign(d);
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                })
                .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        Ok(result)
    }
}

pub struct DateTimeDeserializer<T: DFPrimitiveType> {
    pub builder: PrimitiveArrayBuilder<T>,
    pub tz: Tz,
}

impl<T> TypeDeserializer for DateTimeDeserializer<T>
where
    i64: AsPrimitive<T>,
    T: DFPrimitiveType,
    T: Unmarshal<T> + StatBuffer + FromLexical,
    DFPrimitiveArray<T>: IntoSeries,
{
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

    fn finish_to_series(&mut self) -> Series {
        self.builder.finish().into_series()
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
                let res = self
                    .tz
                    .datetime_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err_to_code(ErrorCode::BadBytes, || {
                        "Cannot parse value to DateTime type"
                    })?;
                self.builder.append_value(res.timestamp().as_());
                Ok(())
            }
        }
    }

    fn de_null(&mut self) {
        self.builder.append_null()
    }
}
