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

use std::cmp;

use super::column_data::ColumnData;
use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::column_data::BoxColumnData;
use crate::types::from_sql::*;
use crate::types::Column;
use crate::types::ColumnType;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub(crate) struct FixedStringColumnData {
    buffer: Vec<u8>,
    str_len: usize,
}

pub(crate) struct FixedStringAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) str_len: usize,
}

pub(crate) struct NullableFixedStringAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
    pub(crate) str_len: usize,
}

impl FixedStringColumnData {
    pub fn with_capacity(capacity: usize, str_len: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity * str_len),
            str_len,
        }
    }

    pub(crate) fn load<T: ReadEx>(reader: &mut T, size: usize, str_len: usize) -> Result<Self> {
        let mut instance = Self::with_capacity(size, str_len);

        for _ in 0..size {
            let old_len = instance.buffer.len();
            instance.buffer.resize(old_len + str_len, 0_u8);
            reader.read_bytes(&mut instance.buffer[old_len..old_len + str_len])?;
        }

        Ok(instance)
    }
}

impl ColumnData for FixedStringColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::FixedString(self.str_len)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let start_index = start * self.str_len;
        let end_index = end * self.str_len;
        encoder.write_bytes(&self.buffer[start_index..end_index]);
    }

    fn len(&self) -> usize {
        self.buffer.len() / self.str_len
    }

    fn push(&mut self, value: Value) {
        let bs: String = String::from(value);
        let l = cmp::min(bs.len(), self.str_len);
        let old_len = self.buffer.len();
        self.buffer.extend_from_slice(&bs.as_bytes()[0..l]);
        self.buffer.resize(old_len + (self.str_len - l), 0_u8);
    }

    fn at(&self, index: usize) -> ValueRef {
        let shift = index * self.str_len;
        let str_ref = &self.buffer[shift..shift + self.str_len];
        ValueRef::String(str_ref)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            buffer: self.buffer.clone(),
            str_len: self.str_len,
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = self.buffer.as_ptr() as *const u8;
        *(pointers[1] as *mut usize) = self.len();
        Ok(())
    }
}

impl<K: ColumnType> ColumnData for FixedStringAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::FixedString(self.str_len)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let mut buffer = Vec::with_capacity(self.str_len);
        for index in start..end {
            buffer.clear();
            match self.column.at(index) {
                ValueRef::String(_) => {
                    let string_ref = self.column.at(index).as_bytes().unwrap();
                    buffer.extend::<&[u8]>(string_ref.as_ref());
                }
                ValueRef::Array(SqlType::UInt8, vs) => {
                    let mut string_val: Vec<u8> = Vec::with_capacity(vs.len());
                    for v in vs.iter() {
                        let byte: u8 = v.clone().into();
                        string_val.push(byte);
                    }
                    let string_ref: &[u8] = string_val.as_ref();
                    buffer.extend(string_ref);
                }
                _ => unimplemented!(),
            }
            buffer.resize(self.str_len, 0);
            encoder.write_bytes(&buffer[..]);
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        self.column.at(index)
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl<K: ColumnType> ColumnData for NullableFixedStringAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Nullable(SqlType::FixedString(self.str_len).into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let size = end - start;
        let mut nulls = vec![0; size];
        let mut values: Vec<Option<&[u8]>> = vec![None; size];

        for (i, index) in (start..end).enumerate() {
            values[i] = Option::from_sql(self.at(index)).unwrap();
            if values[i].is_none() {
                nulls[i] = 1;
            }
        }

        encoder.write_bytes(nulls.as_ref());

        let mut buffer = Vec::with_capacity(self.str_len);
        for value in values {
            buffer.clear();
            if let Some(string_ref) = value {
                buffer.extend(string_ref);
            }
            buffer.resize(self.str_len, 0);
            encoder.write_bytes(buffer.as_ref());
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        self.column.at(index)
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}
