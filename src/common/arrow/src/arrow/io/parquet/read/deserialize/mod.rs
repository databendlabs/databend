// Copyright 2020-2022 Jorge C. Leitão
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

//! APIs to read from Parquet format.
mod binary;
mod binview;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod nested;
mod nested_utils;
mod null;
mod primitive;
mod simple;
mod struct_;
mod utils;

use parquet2::read::get_page_iterator as _get_page_iterator;
use parquet2::schema::types::PrimitiveType;
use simple::page_iter_to_arrays;

pub use self::nested_utils::init_nested;
pub use self::nested_utils::InitNested;
pub use self::nested_utils::NestedArrayIter;
pub use self::nested_utils::NestedState;
pub use self::struct_::StructIterator;
use super::*;
use crate::arrow::array::Array;
use crate::arrow::array::DictionaryKey;
use crate::arrow::array::FixedSizeListArray;
use crate::arrow::array::ListArray;
use crate::arrow::array::MapArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::datatypes::IntervalUnit;
use crate::arrow::error::Result;
use crate::arrow::offset::Offsets;

/// Creates a new iterator of compressed pages.
pub fn get_page_iterator<R: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    reader: R,
    pages_filter: Option<PageFilter>,
    buffer: Vec<u8>,
    max_header_size: usize,
) -> Result<PageReader<R>> {
    Ok(_get_page_iterator(
        column_metadata,
        reader,
        pages_filter,
        buffer,
        max_header_size,
    )?)
}

/// Creates a new [`ListArray`] or [`FixedSizeListArray`].
pub fn create_list(
    data_type: DataType,
    nested: &mut NestedState,
    values: Box<dyn Array>,
) -> Box<dyn Array> {
    let (mut offsets, validity) = nested.nested.pop().unwrap().inner();
    match data_type.to_logical_type() {
        DataType::List(_) => {
            offsets.push(values.len() as i64);

            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();

            let offsets: Offsets<i32> = offsets
                .try_into()
                .expect("i64 offsets do not fit in i32 offsets");

            Box::new(ListArray::<i32>::new(
                data_type,
                offsets.into(),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        DataType::LargeList(_) => {
            offsets.push(values.len() as i64);

            Box::new(ListArray::<i64>::new(
                data_type,
                offsets.try_into().expect("List too large"),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new(
            data_type,
            values,
            validity.and_then(|x| x.into()),
        )),
        _ => unreachable!(),
    }
}

/// Creates a new [`MapArray`].
pub fn create_map(
    data_type: DataType,
    nested: &mut NestedState,
    values: Box<dyn Array>,
) -> Box<dyn Array> {
    let (mut offsets, validity) = nested.nested.pop().unwrap().inner();
    match data_type.to_logical_type() {
        DataType::Map(_, _) => {
            offsets.push(values.len() as i64);
            let offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();

            let offsets: Offsets<i32> = offsets
                .try_into()
                .expect("i64 offsets do not fit in i32 offsets");

            Box::new(MapArray::new(
                data_type,
                offsets.into(),
                values,
                validity.and_then(|x| x.into()),
            ))
        }
        _ => unreachable!(),
    }
}

fn is_primitive(data_type: &DataType) -> bool {
    matches!(
        data_type.to_physical_type(),
        crate::arrow::datatypes::PhysicalType::Primitive(_)
            | crate::arrow::datatypes::PhysicalType::Null
            | crate::arrow::datatypes::PhysicalType::Boolean
            | crate::arrow::datatypes::PhysicalType::Utf8
            | crate::arrow::datatypes::PhysicalType::LargeUtf8
            | crate::arrow::datatypes::PhysicalType::Binary
            | crate::arrow::datatypes::PhysicalType::LargeBinary
            | crate::arrow::datatypes::PhysicalType::BinaryView
            | crate::arrow::datatypes::PhysicalType::Utf8View
            | crate::arrow::datatypes::PhysicalType::FixedSizeBinary
            | crate::arrow::datatypes::PhysicalType::Dictionary(_)
    )
}

fn columns_to_iter_recursive<'a, I>(
    mut columns: Vec<I>,
    mut types: Vec<&PrimitiveType>,
    field: Field,
    init: Vec<InitNested>,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<NestedArrayIter<'a>>
where
    I: Pages + 'a,
{
    if init.is_empty() && is_primitive(&field.data_type) {
        return Ok(Box::new(
            page_iter_to_arrays(
                columns.pop().unwrap(),
                types.pop().unwrap(),
                field.data_type,
                chunk_size,
                num_rows,
            )?
            .map(|x| Ok((NestedState::new(vec![]), x?))),
        ));
    }

    nested::columns_to_iter_recursive(columns, types, field, init, num_rows, chunk_size)
}

/// Returns the number of (parquet) columns that a [`DataType`] contains.
pub fn n_columns(data_type: &DataType) -> usize {
    use crate::arrow::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | Dictionary(_) | LargeUtf8 | BinaryView | Utf8View => 1,
        List | FixedSizeList | LargeList => {
            let a = data_type.to_logical_type();
            if let DataType::List(inner) = a {
                n_columns(&inner.data_type)
            } else if let DataType::LargeList(inner) = a {
                n_columns(&inner.data_type)
            } else if let DataType::FixedSizeList(inner, _) = a {
                n_columns(&inner.data_type)
            } else {
                unreachable!()
            }
        }
        Map => {
            let a = data_type.to_logical_type();
            if let DataType::Map(inner, _) = a {
                n_columns(&inner.data_type)
            } else {
                unreachable!()
            }
        }
        Struct => {
            if let DataType::Struct(fields) = data_type.to_logical_type() {
                fields.iter().map(|inner| n_columns(&inner.data_type)).sum()
            } else {
                unreachable!()
            }
        }
        _ => todo!(),
    }
}

/// An iterator adapter that maps multiple iterators of [`Pages`] into an iterator of [`Array`]s.
///
/// For a non-nested datatypes such as [`DataType::Int32`], this function requires a single element in `columns` and `types`.
/// For nested types, `columns` must be composed by all parquet columns with associated types `types`.
///
/// The arrays are guaranteed to be at most of size `chunk_size` and data type `field.data_type`.
pub fn column_iter_to_arrays<'a, I>(
    columns: Vec<I>,
    types: Vec<&PrimitiveType>,
    field: Field,
    chunk_size: Option<usize>,
    num_rows: usize,
) -> Result<ArrayIter<'a>>
where
    I: Pages + 'a,
{
    Ok(Box::new(
        columns_to_iter_recursive(columns, types, field, vec![], num_rows, chunk_size)?
            .map(|x| x.map(|x| x.1)),
    ))
}

/// Basically the same as `column_iter_to_arrays`, with the addition of the `init` parameter
/// to read the inner columns of the nested type directly, instead of reading the entire nested type.
pub fn nested_column_iter_to_arrays<'a, I>(
    columns: Vec<I>,
    types: Vec<&PrimitiveType>,
    field: Field,
    init: Vec<InitNested>,
    chunk_size: Option<usize>,
    num_rows: usize,
) -> Result<ArrayIter<'a>>
where
    I: Pages + 'a,
{
    Ok(Box::new(
        nested::columns_to_iter_recursive(columns, types, field, init, num_rows, chunk_size)?
            .map(|x| x.map(|x| x.1)),
    ))
}
