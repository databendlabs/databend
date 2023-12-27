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

use parquet2::schema::types::FieldInfo;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;

use super::boolean::write_bitmap;
use super::primitive::write_primitive;
use super::WriteOptions;
use crate::arrow::array::*;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::write::write_def_levels;
use crate::arrow::io::parquet::write::write_rep_and_def;
use crate::arrow::io::parquet::write::Nested;
use crate::arrow::io::parquet::write::Version;
use crate::native::write::binary::write_binary;
use crate::with_match_primitive_type;

/// Writes an [`Array`] to the file
pub fn write<W: Write>(
    w: &mut W,
    array: &dyn Array,
    nested: &[Nested],
    type_: PrimitiveType,
    length: usize,
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if nested.len() == 1 {
        return write_simple(w, array, type_, write_options, scratch);
    }
    write_nested(w, array, nested, length, write_options, scratch)
}

/// Writes an [`Array`] to `arrow_data`
pub fn write_simple<W: Write>(
    w: &mut W,
    array: &dyn Array,
    type_: PrimitiveType,
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    use PhysicalType::*;

    let is_optional = is_nullable(&type_.field_info);
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => {
            let array: &BooleanArray = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_bitmap::<W>(w, array, write_options, scratch)?
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array: &PrimitiveArray<$T> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_primitive::<$T, W>(w, array, write_options, scratch)?;
        }),
        Binary => {
            let array: &BinaryArray<i32> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_binary::<i32, W>(w, array, write_options, scratch)?;
        }
        LargeBinary => {
            let array: &BinaryArray<i64> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_binary::<i64, W>(w, array, write_options, scratch)?;
        }
        Utf8 => {
            let binary_array: &Utf8Array<i32> = array.as_any().downcast_ref().unwrap();

            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }

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

            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }

            let binary_array = BinaryArray::new(
                DataType::LargeBinary,
                binary_array.offsets().clone(),
                binary_array.values().clone(),
                binary_array.validity().cloned(),
            );
            write_binary::<i64, W>(w, &binary_array, write_options, scratch)?;
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

/// Writes a nested [`Array`] to `arrow_data`
pub fn write_nested<W: Write>(
    w: &mut W,
    array: &dyn Array,
    nested: &[Nested],
    length: usize,
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_nested_validity::<W>(w, nested, length, scratch)?;

    scratch.clear();

    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => {
            let array: &BooleanArray = array.as_any().downcast_ref().unwrap();
            write_bitmap::<W>(w, array, write_options, scratch)?
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array = array.as_any().downcast_ref().unwrap();
            write_primitive::<$T, W>(w, array, write_options, scratch)?;
        }),
        Binary => {
            let binary_array: &BinaryArray<i32> = array.as_any().downcast_ref().unwrap();
            write_binary::<i32, W>(w, binary_array, write_options, scratch)?;
        }
        LargeBinary => {
            let binary_array: &BinaryArray<i64> = array.as_any().downcast_ref().unwrap();
            write_binary::<i64, W>(w, binary_array, write_options, scratch)?;
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
                DataType::Binary,
                binary_array.offsets().clone(),
                binary_array.values().clone(),
                binary_array.validity().cloned(),
            );

            write_binary::<i64, W>(w, &binary_array, write_options, scratch)?;
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

fn write_validity<W: Write>(
    w: &mut W,
    is_optional: bool,
    validity: Option<&Bitmap>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();

    write_def_levels(scratch, is_optional, validity, length, Version::V2)?;
    let def_levels_len = scratch.len();
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..def_levels_len])?;

    Ok(())
}

fn write_nested_validity<W: Write>(
    w: &mut W,
    nested: &[Nested],
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();

    let (rep_levels_len, def_levels_len) = write_rep_and_def(Version::V2, nested, scratch)?;
    w.write_all(&(length as u32).to_le_bytes())?;
    w.write_all(&(rep_levels_len as u32).to_le_bytes())?;
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..scratch.len()])?;

    Ok(())
}

fn is_nullable(field_info: &FieldInfo) -> bool {
    match field_info.repetition {
        Repetition::Optional => true,
        Repetition::Repeated => true,
        Repetition::Required => false,
    }
}
