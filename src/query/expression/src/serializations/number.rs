// Copyright 2022 Datafuse Labs.
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

use common_arrow::arrow::buffer::Buffer;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::types::number::Number;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::Column;
use crate::TypeSerializer;

#[derive(Debug, Clone)]
pub struct NumberSerializer<T: Number> {
    pub(crate) values: Buffer<T>,
}

impl<T: Number> NumberSerializer<T> {
    pub fn try_create(col: Column) -> Result<Self, String> {
        let column = col
            .into_number()
            .map_err(|_| "unable to get number column".to_string())?;
        let values = T::try_downcast_column(&column).ok_or("downcast column failed")?;

        Ok(Self { values })
    }
}

impl<T> TypeSerializer for NumberSerializer<T>
where T: Number + PrimitiveWithFormat
{
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.values[row_index].write_field(buf, format)
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>, String> {
        let result: Vec<Value> = self.values.iter().map(|x| x.to_value()).collect();
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
    fn to_value(self) -> Value;
}

macro_rules! impl_float {
    ($ty:ident) => {
        impl PrimitiveWithFormat for $ty {
            fn write_field(self: $ty, buf: &mut Vec<u8>, format: &FormatSettings) {
                // todo(youngsofun): output the sign optionally
                match self.0.classify() {
                    FpCategory::Nan => {
                        buf.extend_from_slice(&format.nan_bytes);
                    }
                    FpCategory::Infinite => {
                        buf.extend_from_slice(&format.inf_bytes);
                    }
                    _ => {
                        extend_lexical(self.0, buf);
                    }
                }
            }

            fn to_value(self: $ty) -> Value {
                serde_json::to_value(self.0).unwrap()
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

            fn to_value(self: $ty) -> Value {
                serde_json::to_value(self).unwrap()
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
impl_float!(F32);
impl_float!(F64);
