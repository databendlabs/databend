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

use std::num::FpCategory;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use micromarshal::Marshal;
use micromarshal::Unmarshal;
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use opensrv_clickhouse::types::HasSqlType;
use serde_json::Value;

use crate::ColumnRef;
use crate::PrimitiveColumn;
use crate::PrimitiveType;
use crate::Series;
use crate::TypeSerializer;

#[derive(Debug, Clone)]
pub struct NumberSerializer<'a, T: PrimitiveType> {
    pub(crate) values: &'a [T],
}

impl<'a, T: PrimitiveType> NumberSerializer<'a, T> {
    pub fn try_create(col: &'a ColumnRef) -> Result<Self> {
        let col: &PrimitiveColumn<T> = Series::check_get(col)?;
        Ok(NumberSerializer {
            values: col.values(),
        })
    }
}

impl<'a, T> TypeSerializer<'a> for NumberSerializer<'a, T>
where T: PrimitiveType
        + opensrv_clickhouse::types::StatBuffer
        + Marshal
        + Unmarshal<T>
        + HasSqlType
        + std::convert::Into<opensrv_clickhouse::types::Value>
        + std::convert::From<opensrv_clickhouse::types::Value>
        + lexical_core::ToLexical
        + PrimitiveWithFormat
{
    fn need_quote(&self) -> bool {
        false
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.values[row_index].write_field(buf, format)
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self
            .values
            .iter()
            .map(|x| serde_json::to_value(x).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_const(
        &self,
        _format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let mut values: Vec<T> = Vec::with_capacity(self.values.len() * size);
        for _ in 0..size {
            for v in self.values.iter() {
                values.push(*v)
            }
        }
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_clickhouse_column(
        &self,
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let values: Vec<T> = self.values.iter().map(|c| c.to_owned()).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_json_object(
        &self,
        _valids: Option<&Bitmap>,
        format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        self.serialize_json_values(format)
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let result: Vec<Option<Value>> = self
            .values
            .iter()
            .map(|x| match serde_json::to_value(x) {
                Ok(v) => Some(v),
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }
}

// 30% faster lexical_core::write to tmp buf and extend_from_slice
#[inline]
pub fn extend_lexical<N: lexical_core::ToLexical>(n: N, buf: &mut Vec<u8>) {
    buf.reserve(N::FORMATTED_SIZE_DECIMAL);
    let len0 = buf.len();
    unsafe {
        let slice =
            std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(len0), buf.capacity() - len0);
        let len = lexical_core::write(n, slice).len();
        buf.set_len(len0 + len);
    }
}

trait PrimitiveWithFormat {
    fn write_field(self, buf: &mut Vec<u8>, _format: &FormatSettings);
}

macro_rules! impl_float {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, buf: &mut Vec<u8>, format: &FormatSettings) {
                // todo(youngsofun): output the sign optionally
                match self.classify() {
                    FpCategory::Nan => {
                        buf.extend_from_slice(&format.nan_bytes);
                    }
                    FpCategory::Infinite => {
                        buf.extend_from_slice(&format.inf_bytes);
                    }
                    _ => {
                        extend_lexical(self, buf);
                    }
                }
            }
        }
    };
}

macro_rules! impl_int {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, buf: &mut Vec<u8>, _format: &FormatSettings) {
                extend_lexical(self, buf);
            }
        }
    };
}

impl_int!(i8);
impl_int!(i16);
impl_int!(i32);
impl_int!(i64);
impl_int!(u8);
impl_int!(u16);
impl_int!(u32);
impl_int!(u64);
impl_float!(f32);
impl_float!(f64);
