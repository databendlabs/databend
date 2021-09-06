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
// limitations under the License

use std::convert;
use std::mem;
use std::sync::Arc;

use super::column_data::BoxColumnData;
use super::column_data::ColumnData;
use super::list::List;
use super::ColumnFrom;
use crate::binary::Encoder;
use crate::binary::ReadEx;
use crate::errors::Result;
use crate::types::column::array::ArrayColumnData;
use crate::types::column::nullable::NullableColumnData;
use crate::types::column::ArcColumnWrapper;
use crate::types::column::ColumnWrapper;
use crate::types::HasSqlType;
use crate::types::Marshal;
use crate::types::SqlType;
use crate::types::StatBuffer;
use crate::types::Unmarshal;
use crate::types::Value;
use crate::types::ValueRef;

pub struct VectorColumnData<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Sync
        + HasSqlType
        + 'static
{
    pub(crate) data: List<T>,
}

impl<T> ColumnFrom for Vec<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + HasSqlType
        + 'static
{
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::with_capacity(source.len());
        for s in source {
            data.push(s);
        }
        W::wrap(VectorColumnData { data })
    }
}

impl<T> ColumnFrom for Vec<Option<T>>
where
    Value: convert::From<T>,
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + HasSqlType
        + 'static,
{
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake: Vec<T> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);

        let mut data = NullableColumnData {
            inner,
            nulls: Vec::with_capacity(source.len()),
        };

        for value in source {
            data.push(value.into());
        }

        W::wrap(data)
    }
}

impl<T> ColumnFrom for Vec<Vec<T>>
where
    Value: convert::From<T>,
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + HasSqlType
        + 'static,
{
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake: Vec<T> = Vec::with_capacity(source.len());
        let inner = Vec::column_from::<ArcColumnWrapper>(fake);
        let sql_type = inner.sql_type();

        let mut data = ArrayColumnData {
            inner,
            offsets: List::with_capacity(source.len()),
        };

        for array in source {
            data.push(to_array(sql_type.clone(), array));
        }

        W::wrap(data)
    }
}

fn to_array<T>(sql_type: SqlType, vs: Vec<T>) -> Value
where
    Value: convert::From<T>,
    T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + HasSqlType
        + 'static,
{
    let mut inner = Vec::with_capacity(vs.len());
    for v in vs {
        let value: Value = v.into();
        inner.push(value)
    }
    Value::Array(sql_type.into(), Arc::new(inner))
}

impl<T> VectorColumnData<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Sync
        + HasSqlType
        + 'static
{
    pub(crate) fn with_capacity(capacity: usize) -> VectorColumnData<T> {
        VectorColumnData {
            data: List::with_capacity(capacity),
        }
    }

    pub(crate) fn load<R: ReadEx>(reader: &mut R, size: usize) -> Result<VectorColumnData<T>> {
        let mut data = List::with_capacity(size);
        unsafe {
            data.set_len(size);
        }
        reader.read_bytes(data.as_mut())?;
        Ok(Self { data })
    }
}

impl<T> ColumnData for VectorColumnData<T>
where T: StatBuffer
        + Unmarshal<T>
        + Marshal
        + Copy
        + convert::Into<Value>
        + convert::From<Value>
        + Send
        + Sync
        + HasSqlType
        + 'static
{
    fn sql_type(&self) -> SqlType {
        T::sql_type()
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        save_data::<T>(self.data.as_ref(), encoder, start, end);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn push(&mut self, value: Value) {
        self.data.push(T::from(value));
    }

    fn at(&self, index: usize) -> ValueRef {
        let v: Value = self.data.at(index).into();
        match v {
            Value::UInt8(x) => ValueRef::UInt8(x),
            Value::UInt16(x) => ValueRef::UInt16(x),
            Value::UInt32(x) => ValueRef::UInt32(x),
            Value::UInt64(x) => ValueRef::UInt64(x),

            Value::Int8(x) => ValueRef::Int8(x),
            Value::Int16(x) => ValueRef::Int16(x),
            Value::Int32(x) => ValueRef::Int32(x),
            Value::Int64(x) => ValueRef::Int64(x),

            Value::Float32(x) => ValueRef::Float32(x),
            Value::Float64(x) => ValueRef::Float64(x),

            _ => panic!("can't convert value to value_ref."),
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            data: self.data.clone(),
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = self.data.as_ptr() as *const u8;
        *(pointers[1] as *mut usize) = self.len();
        Ok(())
    }
}

pub(crate) fn save_data<T>(data: &[u8], encoder: &mut Encoder, start: usize, end: usize) {
    let start_index = start * mem::size_of::<T>();
    let end_index = end * mem::size_of::<T>();
    encoder.write_bytes(&data[start_index..end_index]);
}
