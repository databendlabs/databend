// Copyright 2021 Datafuse Labs
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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::NativeType;
use databend_common_column::types::PrimitiveType;
use databend_common_column::types::i256;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;

use super::WriteOptions;
use crate::compression::double::compress_double;
use crate::compression::integer::compress_integer;
use crate::error::Result;

pub(crate) fn write_primitive<T: NativeType, W: Write>(
    w: &mut W,
    col: &Buffer<T>,
    validity: Option<Bitmap>,
    write_options: &WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();
    match T::PRIMITIVE {
        PrimitiveType::Int8 => {
            let array: &Buffer<i8> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Int16 => {
            let array: &Buffer<i16> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Int32 => {
            let array: &Buffer<i32> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Int64 => {
            let array: &Buffer<i64> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::UInt8 => {
            let array: &Buffer<u8> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::UInt16 => {
            let array: &Buffer<u16> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::UInt32 => {
            let array: &Buffer<u32> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::UInt64 => {
            let array: &Buffer<u64> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Int128 => {
            let array: &Buffer<i128> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Int256 => {
            let array: &Buffer<i256> = unsafe { std::mem::transmute(col) };
            compress_integer(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Float32 => {
            let array: &Buffer<F32> = unsafe { std::mem::transmute(col) };
            compress_double(array, validity, write_options, scratch)?;
        }
        PrimitiveType::Float64 => {
            let array: &Buffer<F64> = unsafe { std::mem::transmute(col) };
            compress_double(array, validity, write_options, scratch)?;
        }

        PrimitiveType::Float16 => unimplemented!(),
        PrimitiveType::DaysMs => unimplemented!(),
        PrimitiveType::MonthDayMicros => unimplemented!(),
        PrimitiveType::TimestampTz => unimplemented!(),
        PrimitiveType::UInt128 => unimplemented!(),
    }
    w.write_all(scratch.as_slice())?;
    Ok(())
}
