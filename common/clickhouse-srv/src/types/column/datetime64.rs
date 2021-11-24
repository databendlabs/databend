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

use chrono::prelude::*;
use chrono_tz::Tz;

use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::column_data::BoxColumnData;
use crate::types::column::column_data::ColumnData;
use crate::types::column::list::List;
use crate::types::DateTimeType;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub struct DateTime64ColumnData {
    data: List<i64>,
    params: (u32, Tz),
}

impl DateTime64ColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        size: usize,
        precision: u32,
        tz: Tz,
    ) -> Result<DateTime64ColumnData> {
        let mut data = List::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        reader.read_bytes(data.as_mut())?;
        Ok(DateTime64ColumnData {
            data,
            params: (precision, tz),
        })
    }

    pub(crate) fn with_capacity(capacity: usize, precision: u32, timezone: Tz) -> Self {
        DateTime64ColumnData {
            data: List::with_capacity(capacity),
            params: (precision, timezone),
        }
    }
}

impl ColumnData for DateTime64ColumnData {
    fn sql_type(&self) -> SqlType {
        let (precision, tz) = self.params;
        SqlType::DateTime(DateTimeType::DateTime64(precision, tz))
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for i in start..end {
            encoder.write(self.data.at(i));
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        let (precision, tz) = &self.params;
        let time = DateTime::<Tz>::from(value);
        let stamp = from_datetime(time.with_timezone(tz), *precision);
        self.data.push(stamp)
    }

    fn at(&self, index: usize) -> ValueRef {
        let value = self.data.at(index);
        ValueRef::DateTime64(value, &self.params)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
            params: self.params,
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        let (precision, tz) = &self.params;
        *pointers[0] = self.data.as_ptr() as *const u8;
        *pointers[1] = tz as *const Tz as *const u8;
        *(pointers[2] as *mut usize) = self.len();
        *(pointers[3] as *mut Option<u32>) = Some(*precision);
        Ok(())
    }
}

pub fn from_datetime<T: chrono::offset::TimeZone>(time: DateTime<T>, precision: u32) -> i64 {
    let base10: i64 = 10;
    let timestamp = time.timestamp_nanos();
    timestamp / base10.pow(9 - precision)
}

#[inline(always)]
pub fn to_datetime(value: i64, precision: u32, tz: Tz) -> DateTime<Tz> {
    let base10: i64 = 10;

    let nano = if precision < 19 {
        value * base10.pow(9 - precision)
    } else {
        0_i64
    };

    let sec = nano / 1_000_000_000;
    let nsec = nano - sec * 1_000_000_000;

    tz.timestamp(sec, nsec as u32)
}
