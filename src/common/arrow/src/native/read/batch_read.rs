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

use parquet2::metadata::ColumnDescriptor;

use super::array::*;
use super::NativeReadBuf;
use crate::arrow::array::*;
use crate::arrow::compute::concatenate::concatenate;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::create_list;
use crate::arrow::io::parquet::read::create_map;
use crate::arrow::io::parquet::read::n_columns;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;
use crate::native::PageMeta;

pub fn read_simple<R: NativeReadBuf>(
    reader: &mut R,
    field: Field,
    page_metas: Vec<PageMeta>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;

    let is_nullable = field.is_nullable;
    let data_type = field.data_type().clone();

    match data_type.to_physical_type() {
        Null => read_null(data_type, page_metas),
        Boolean => read_boolean(reader, is_nullable, data_type, page_metas),
        Primitive(primitive) => with_match_integer_double_type!(primitive,
        |$T| {
            read_integer::<$T, _>(
                reader,
                is_nullable,
                data_type,
                page_metas,
            )
        },
        |$T| {
            read_double::<$T, _>(
                reader,
                is_nullable,
                data_type,
                page_metas,
            )
        }),
        Binary | Utf8 => read_binary::<i32, _>(reader, is_nullable, data_type, page_metas),
        LargeBinary | LargeUtf8 => {
            read_binary::<i64, _>(reader, is_nullable, data_type, page_metas)
        }
        FixedSizeBinary => unimplemented!(),
        _ => unreachable!(),
    }
}

pub fn read_nested<R: NativeReadBuf>(
    mut readers: Vec<R>,
    field: Field,
    mut leaves: Vec<ColumnDescriptor>,
    mut init: Vec<InitNested>,
    mut page_metas: Vec<Vec<PageMeta>>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    use PhysicalType::*;

    Ok(match field.data_type().to_physical_type() {
        Null => unimplemented!(),
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_boolean(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Primitive(primitive) => with_match_integer_double_type!(primitive,
        |$T| {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_integer::<$T, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        },
        |$T| {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_primitive::<$T, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        ),
        Binary | Utf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_binary::<i32, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        LargeBinary | LargeUtf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            read_nested_binary::<i64, _>(
                &mut readers.pop().unwrap(),
                field.data_type().clone(),
                leaves.pop().unwrap(),
                init,
                page_metas.pop().unwrap(),
            )?
        }

        FixedSizeBinary => unimplemented!(),
        _ => match field.data_type().to_logical_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let results =
                    read_nested(readers, inner.as_ref().clone(), leaves, init, page_metas)?;
                let mut arrays = Vec::with_capacity(results.len());
                for (mut nested, values) in results {
                    let array = create_list(field.data_type().clone(), &mut nested, values);
                    arrays.push((nested, array));
                }
                arrays
            }
            DataType::Map(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let results =
                    read_nested(readers, inner.as_ref().clone(), leaves, init, page_metas)?;
                let mut arrays = Vec::with_capacity(results.len());
                for (mut nested, values) in results {
                    let array = create_map(field.data_type().clone(), &mut nested, values);
                    arrays.push((nested, array));
                }
                arrays
            }
            DataType::Struct(fields) => {
                let mut results = fields
                    .iter()
                    .map(|f| {
                        let mut init = init.clone();
                        init.push(InitNested::Struct(field.is_nullable));
                        let n = n_columns(&f.data_type);
                        let readers = readers.drain(..n).collect();
                        let leaves = leaves.drain(..n).collect();
                        let page_metas = page_metas.drain(..n).collect();
                        read_nested(readers, f.clone(), leaves, init, page_metas)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut arrays = Vec::with_capacity(results[0].len());
                while !results[0].is_empty() {
                    let mut nesteds = Vec::with_capacity(fields.len());
                    let mut values = Vec::with_capacity(fields.len());
                    for result in results.iter_mut() {
                        let (nested, value) = result.pop().unwrap();
                        nesteds.push(nested);
                        values.push(value);
                    }
                    let array = create_struct(fields.clone(), &mut nesteds, values);
                    arrays.push(array);
                }
                arrays.reverse();
                arrays
            }
            _ => unreachable!(),
        },
    })
}

/// Read all pages of column at once.
pub fn batch_read_array<R: NativeReadBuf>(
    mut readers: Vec<R>,
    leaves: Vec<ColumnDescriptor>,
    field: Field,
    is_nested: bool,
    mut page_metas: Vec<Vec<PageMeta>>,
) -> Result<Box<dyn Array>> {
    if is_nested {
        let results = read_nested(readers, field, leaves, vec![], page_metas)?;
        let arrays: Vec<&dyn Array> = results.iter().map(|(_, v)| v.as_ref()).collect();
        let array = concatenate(&arrays).unwrap();
        Ok(array)
    } else {
        read_simple(
            &mut readers.pop().unwrap(),
            field,
            page_metas.pop().unwrap(),
        )
    }
}
