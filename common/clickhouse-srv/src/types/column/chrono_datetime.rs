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

use std::ptr;
use std::slice;
use std::sync::Arc;

use chrono::prelude::*;
use chrono_tz::Tz;

use crate::binary::Encoder;
use crate::errors::Result;
use crate::types::column::column_data::ArcColumnData;
use crate::types::column::column_data::BoxColumnData;
use crate::types::column::column_data::ColumnData;
use crate::types::column::datetime64::from_datetime;
use crate::types::column::nullable::NullableColumnData;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::ColumnFrom;
use crate::types::column::ColumnWrapper;
use crate::types::column::SqlType;
use crate::types::column::Value;
use crate::types::column::ValueRef;
use crate::types::DateTimeType;

pub struct ChronoDateTimeColumnData {
    data: Vec<DateTime<Tz>>,
    tz: Tz,
}

pub(crate) struct ChronoDateTimeAdapter {
    pub(crate) column: ArcColumnData,
    dst_type: SqlType,
}

impl ChronoDateTimeAdapter {
    pub(crate) fn new(column: ArcColumnData, dst_type: SqlType) -> ChronoDateTimeAdapter {
        ChronoDateTimeAdapter { column, dst_type }
    }
}

impl ColumnFrom for Vec<DateTime<Tz>> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        let tz = if data.is_empty() {
            Tz::Zulu
        } else {
            data[0].timezone()
        };
        W::wrap(ChronoDateTimeColumnData { data, tz })
    }
}

impl ColumnFrom for Vec<Option<DateTime<Tz>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        let n = source.len();

        let tz = source
            .iter()
            .find_map(|u| u.map(|v| v.timezone()))
            .unwrap_or(Tz::Zulu);

        let mut values: Vec<DateTime<Tz>> = Vec::with_capacity(n);
        let mut nulls = Vec::with_capacity(n);

        for time in source {
            match time {
                None => {
                    nulls.push(1);
                    values.push(tz.timestamp(0, 0))
                }
                Some(time) => {
                    nulls.push(0);
                    values.push(time)
                }
            }
        }

        W::wrap(NullableColumnData {
            inner: Vec::column_from::<ArcColumnWrapper>(values),
            nulls,
        })
    }
}

impl ColumnData for ChronoDateTimeColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::DateTime(DateTimeType::Chrono)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for datetime in &self.data[start..end] {
            let value = datetime.with_timezone(&self.tz).timestamp() as u32;
            encoder.write(value);
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        let time: DateTime<Tz> = value.into();
        self.data.push(time);
    }

    fn at(&self, index: usize) -> ValueRef {
        let v = &self.data[index];
        ValueRef::DateTime(v.timestamp() as u32, v.timezone())
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
            tz: self.tz,
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = self.data.as_ptr() as *const u8;
        *pointers[1] = &self.tz as *const Tz as *const u8;
        *(pointers[2] as *mut usize) = self.len();
        Ok(())
    }

    fn cast_to(&self, this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if target.is_datetime() {
            let clone = this.clone();
            let adapter = ChronoDateTimeAdapter::new(clone, target.clone());
            Some(Arc::new(adapter))
        } else {
            None
        }
    }
}

#[inline(always)]
fn is_chrono_datetime(column: &dyn ColumnData) -> bool {
    column.sql_type() == SqlType::DateTime(DateTimeType::Chrono)
}

pub(crate) fn get_date_slice<'a>(column: &dyn ColumnData) -> Result<&'a [DateTime<Tz>]> {
    unsafe {
        let mut data: *const DateTime<Tz> = ptr::null();
        let mut tz: *const Tz = ptr::null();
        let mut len: usize = 0;
        column.get_internal(
            &[
                &mut data as *mut *const DateTime<Tz> as *mut *const u8,
                &mut tz as *mut *const Tz as *mut *const u8,
                &mut len as *mut usize as *mut *const u8,
            ],
            0,
        )?;
        assert_ne!(data, ptr::null());
        assert_ne!(tz, ptr::null());
        Ok(slice::from_raw_parts(data, len))
    }
}

impl ColumnData for ChronoDateTimeAdapter {
    fn sql_type(&self) -> SqlType {
        self.dst_type.clone()
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        if !is_chrono_datetime(self.column.as_ref()) {
            panic!("Invalid column type {}.", self.column.sql_type());
        }

        let dates = get_date_slice(self.column.as_ref()).unwrap();

        match self.dst_type {
            SqlType::DateTime(DateTimeType::DateTime64(precision, tz)) => {
                for date in &dates[start..end] {
                    let value = from_datetime(date.with_timezone(&tz), precision);
                    encoder.write(value);
                }
            }
            SqlType::DateTime(DateTimeType::DateTime32) => {
                for date in &dates[start..end] {
                    let value = date.timestamp() as u32;
                    encoder.write(value);
                }
            }
            _ => unimplemented!(),
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, _index: usize) -> ValueRef {
        unimplemented!()
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }

    unsafe fn get_internal(&self, _pointers: &[*mut *const u8], _level: u8) -> Result<()> {
        unimplemented!()
    }
}
