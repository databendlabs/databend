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

use std::io::Write;
use std::string::ToString;
use std::sync::Arc;

use super::column_data::BoxColumnData;
use super::column_data::ColumnData;
use super::ColumnFrom;
use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::array::ArrayColumnData;
use crate::types::column::list::List;
use crate::types::column::nullable::NullableColumnData;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::ColumnWrapper;
use crate::types::column::Either;
use crate::types::column::StringPool;
use crate::types::Column;
use crate::types::ColumnType;
use crate::types::FromSql;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub struct StringColumnData {
    pool: StringPool,
}

pub(crate) struct StringAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
}

impl StringColumnData {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            pool: StringPool::with_capacity(capacity),
        }
    }

    pub fn load<T: ReadEx>(reader: &mut T, size: usize) -> Result<Self> {
        let mut data = Self::with_capacity(size);

        for _ in 0..size {
            reader.read_str_into_buffer(&mut data.pool)?;
        }

        Ok(data)
    }
}

impl ColumnFrom for Vec<String> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        W::wrap(StringColumnData { pool: data.into() })
    }
}

impl<'a> ColumnFrom for Vec<&'a str> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let data: Vec<_> = source.iter().map(ToString::to_string).collect();
        W::wrap(StringColumnData { pool: data.into() })
    }
}

impl<'a> ColumnFrom for Vec<&'a [u8]> {
    fn column_from<W: ColumnWrapper>(data: Self) -> W::Wrapper {
        W::wrap(StringColumnData { pool: data.into() })
    }
}

trait StringSource {
    fn into_value(self) -> Value;
}

impl StringSource for String {
    fn into_value(self) -> Value {
        self.into()
    }
}

impl StringSource for &str {
    fn into_value(self) -> Value {
        self.into()
    }
}

impl StringSource for Vec<u8> {
    fn into_value(self) -> Value {
        Value::String(Arc::new(self))
    }
}

impl ColumnFrom for Vec<Vec<String>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        make_array_of_array::<W, String>(source)
    }
}

impl ColumnFrom for Vec<Vec<&str>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> <W as ColumnWrapper>::Wrapper {
        make_array_of_array::<W, &str>(source)
    }
}

fn make_array_of_array<W: ColumnWrapper, S: StringSource>(
    source: Vec<Vec<S>>,
) -> <W as ColumnWrapper>::Wrapper {
    let fake: Vec<String> = Vec::with_capacity(source.len());
    let inner = Vec::column_from::<ArcColumnWrapper>(fake);
    let sql_type = inner.sql_type();

    let mut data = ArrayColumnData {
        inner,
        offsets: List::with_capacity(source.len()),
    };

    for vs in source {
        let mut inner = Vec::with_capacity(vs.len());
        for v in vs {
            let value: Value = v.into_value();
            inner.push(value)
        }
        data.push(Value::Array(sql_type.clone().into(), Arc::new(inner)));
    }

    W::wrap(data)
}

impl ColumnFrom for Vec<Option<Vec<u8>>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        make_opt_column::<W, Vec<u8>>(source)
    }
}

impl ColumnFrom for Vec<Option<&str>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        make_opt_column::<W, &str>(source)
    }
}

impl ColumnFrom for Vec<Option<String>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        make_opt_column::<W, String>(source)
    }
}

fn make_opt_column<W: ColumnWrapper, S: StringSource>(source: Vec<Option<S>>) -> W::Wrapper {
    let inner = Arc::new(StringColumnData::with_capacity(source.len()));

    let mut data = NullableColumnData {
        inner,
        nulls: Vec::with_capacity(source.len()),
    };

    for value in source {
        let item = if let Some(v) = value {
            let inner = v.into_value();
            Value::Nullable(Either::Right(Box::new(inner)))
        } else {
            Value::Nullable(Either::Left(SqlType::String.into()))
        };
        data.push(item);
    }

    W::wrap(data)
}

impl ColumnData for StringColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::String
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let strings = self.pool.strings();
        for v in strings.skip(start).take(end - start) {
            encoder.byte_string(v);
        }
    }

    fn len(&self) -> usize {
        self.pool.len()
    }

    fn push(&mut self, value: Value) {
        let s: Vec<u8> = value.into();
        let mut b = self.pool.allocate(s.len());
        b.write_all(s.as_ref()).unwrap();
    }

    fn at(&self, index: usize) -> ValueRef {
        let s = self.pool.get(index);
        ValueRef::from(s)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            pool: self.pool.clone(),
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = &self.pool as *const StringPool as *const u8;
        *(pointers[1] as *mut usize) = self.len();
        Ok(())
    }
}

impl<K: ColumnType> ColumnData for StringAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::String
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for index in start..end {
            let buf: Vec<u8> = Vec::from_sql(self.column.at(index)).unwrap();
            encoder.byte_string(buf);
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
