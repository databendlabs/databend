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

use super::boolean::write_bitmap;
use super::primitive::write_primitive;
use super::WriteOptions;
use crate::arrow::array::*;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::native::nested::Nested;
use crate::native::util::encode_bool;
use crate::native::write::binary::write_binary;
use crate::native::write::view::write_view;
use crate::with_match_primitive_type;

/// Writes an [`Array`] to the file
pub fn write<W: Write>(
    w: &mut W,
    array: &dyn Array,
    nested: &[Nested],
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    use PhysicalType::*;
    write_nest_info::<W>(w, nested)?;
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => {
            let array: &BooleanArray = array.as_any().downcast_ref().unwrap();
            write_bitmap::<W>(w, array, write_options, scratch)?
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array: &PrimitiveArray<$T> = array.as_any().downcast_ref().unwrap();
            write_primitive::<$T, W>(w, array, write_options, scratch)?;
        }),
        Binary => {
            let array: &BinaryArray<i32> = array.as_any().downcast_ref().unwrap();
            write_binary::<i32, W>(w, array, write_options, scratch)?;
        }
        LargeBinary => {
            let array: &BinaryArray<i64> = array.as_any().downcast_ref().unwrap();
            write_binary::<i64, W>(w, array, write_options, scratch)?;
        }
        Utf8 => {
            let binary_array: &Utf8Array<i32> = array.as_any().downcast_ref().unwrap();
            let binary_array = BinaryArray::new(
                DataType::Binary,
                binary_array.offsets().clone(),
                binary_array.values().clone(),
                binary_array.validity().cloned(),
            );
            write_binary::<i32, W>(w, &binary_array, write_options, scratch)?;
        }
        LargeUtf8 => {
            let binary_array: &Utf8Array<i64> = array.as_any().downcast_ref().unwrap();

            let binary_array = BinaryArray::new(
                DataType::LargeBinary,
                binary_array.offsets().clone(),
                binary_array.values().clone(),
                binary_array.validity().cloned(),
            );
            write_binary::<i64, W>(w, &binary_array, write_options, scratch)?;
        }
        BinaryView => {
            let array: &BinaryViewArray = array.as_any().downcast_ref().unwrap();
            write_view::<W>(w, array, write_options, scratch)?;
        }
        Utf8View => {
            let array: &Utf8ViewArray = array.as_any().downcast_ref().unwrap();
            let array = array.clone().to_binview();
            write_view::<W>(w, &array, write_options, scratch)?;
        }
        Struct => unreachable!(),
        List => unreachable!(),
        FixedSizeList => unreachable!(),
        Dictionary(_key_type) => unreachable!(),
        Union => unreachable!(),
        Map => unreachable!(),
        _ => todo!(),
    }

    Ok(())
}

fn write_nest_info<W: Write>(w: &mut W, nesteds: &[Nested]) -> Result<()> {
    let is_simple = nesteds.len() == 1;

    if is_simple {
        let nest = nesteds.last().unwrap();

        if nest.is_nullable() {
            let (_, validity) = nest.inner();
            if let Some(bitmap) = validity {
                w.write_all(&(bitmap.len() as u32).to_le_bytes())?;
                let (s, offset, _) = bitmap.as_slice();
                if offset == 0 {
                    w.write_all(s)?;
                } else {
                    encode_bool(w, bitmap.iter())?;
                }
            } else {
                w.write_all(&0u32.to_le_bytes())?;
            }
        }
    } else {
        for nested in nesteds {
            let (values, validity) = nested.inner();

            if nested.is_nullable() {
                if let Some(bitmap) = validity {
                    w.write_all(&(bitmap.len() as u32).to_le_bytes())?;
                    let (s, offset, _) = bitmap.as_slice();
                    if offset == 0 {
                        w.write_all(s)?;
                    } else {
                        encode_bool(w, bitmap.iter())?;
                    }
                } else {
                    w.write_all(&0u32.to_le_bytes())?;
                }
            }

            if nested.is_list() {
                w.write_all(&(values.len() as u32).to_le_bytes())?;
                let input_buf: &[u8] = bytemuck::cast_slice(&values);
                w.write_all(input_buf)?;
            }
        }
    }

    Ok(())
}
