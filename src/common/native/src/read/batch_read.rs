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

use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;

use super::array::*;
use super::NativeReadBuf;
use crate::error::Result;
use crate::nested::create_fixed_list;
use crate::nested::create_list;
use crate::nested::create_map;
use crate::nested::create_struct;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::util::n_columns;
use crate::PageMeta;

fn read_nested_column<R: NativeReadBuf>(
    mut readers: Vec<R>,
    data_type: TableDataType,
    mut init: Vec<InitNested>,
    mut page_metas: Vec<Vec<PageMeta>>,
) -> Result<Vec<(NestedState, Column)>> {
    let is_nullable = data_type.is_nullable();
    use TableDataType::*;
    let result = match data_type.remove_nullable() {
        Null | EmptyArray | EmptyMap => {
            unimplemented!("Can't store pure nulls")
        }
        Boolean => {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_boolean(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Number(number) => with_match_integer_double_type!(number,
        |$T| {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_integer::<NumberType<$T>, $T, _>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        },
        |$T| {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_primitive::<$T, _>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        ),
        Decimal(decimal) => match decimal {
            DecimalDataType::Decimal64(decimal_size) => {
                init.push(InitNested::Primitive(is_nullable));
                read_nested_decimal::<i64, i64, _>(
                    &mut readers.pop().unwrap(),
                    data_type.clone(),
                    decimal_size,
                    init,
                    page_metas.pop().unwrap(),
                )?
            }
            DecimalDataType::Decimal128(decimal_size) => {
                init.push(InitNested::Primitive(is_nullable));
                let mut results = read_nested_decimal::<i128, i128, _>(
                    &mut readers.pop().unwrap(),
                    data_type.clone(),
                    decimal_size,
                    init,
                    page_metas.pop().unwrap(),
                )?;
                if decimal_size.can_carried_by_64() {
                    for (_, col) in results.iter_mut() {
                        if data_type.is_nullable() {
                            let decimal = col
                                .as_nullable_mut()
                                .unwrap()
                                .column
                                .as_decimal_mut()
                                .unwrap();
                            *decimal = decimal.clone().strict_decimal();
                        } else {
                            let decimal = col.as_decimal_mut().unwrap();
                            *decimal = decimal.clone().strict_decimal();
                        }
                    }
                }

                results
            }
            DecimalDataType::Decimal256(decimal_size) => {
                init.push(InitNested::Primitive(is_nullable));
                read_nested_decimal::<
                    databend_common_column::types::i256,
                    databend_common_expression::types::i256,
                    _,
                >(
                    &mut readers.pop().unwrap(),
                    data_type.clone(),
                    decimal_size,
                    init,
                    page_metas.pop().unwrap(),
                )?
            }
        },
        Interval => {
            init.push(InitNested::Primitive(is_nullable));

            read_nested_interval::<_>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        TimestampTz => {
            init.push(InitNested::Primitive(is_nullable));

            read_nested_timestamp_tz::<_>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Timestamp => {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_integer::<TimestampType, _, _>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Date => {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_integer::<DateType, _, _>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        t if t.is_physical_binary() => {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_binary::<_>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }

        String => {
            init.push(InitNested::Primitive(is_nullable));
            read_nested_view_col::<_>(
                &mut readers.pop().unwrap(),
                data_type.clone(),
                init,
                page_metas.pop().unwrap(),
            )?
        }
        Array(inner) => {
            init.push(InitNested::List(is_nullable));
            let results = read_nested_column(readers, inner.as_ref().clone(), init, page_metas)?;
            let mut columns = Vec::with_capacity(results.len());
            for (mut nested, values) in results {
                let array = create_list(data_type.clone(), &mut nested, values);
                columns.push((nested, array));
            }
            columns
        }
        Vector(vector_ty) => {
            init.push(InitNested::FixedList(is_nullable));
            let dimension = vector_ty.dimension() as usize;
            let inner_ty = vector_ty.inner_data_type();
            let results = read_nested_column(readers, inner_ty, init, page_metas)?;
            let mut columns = Vec::with_capacity(results.len());
            for (mut nested, values) in results {
                let array = create_fixed_list(data_type.clone(), dimension, &mut nested, values);
                columns.push((nested, array));
            }
            columns
        }
        Map(inner) => {
            init.push(InitNested::List(is_nullable));
            let results = read_nested_column(readers, inner.as_ref().clone(), init, page_metas)?;
            let mut columns = Vec::with_capacity(results.len());
            for (mut nested, values) in results {
                let array = create_map(data_type.clone(), &mut nested, values);
                columns.push((nested, array));
            }
            columns
        }
        Tuple {
            fields_name: _,
            fields_type,
        } => {
            let mut results = fields_type
                .iter()
                .map(|f| {
                    let mut init = init.clone();
                    init.push(InitNested::Struct(is_nullable));
                    let n = n_columns(f);
                    let readers = readers.drain(..n).collect();
                    let page_metas = page_metas.drain(..n).collect();
                    read_nested_column(readers, f.clone(), init, page_metas)
                })
                .collect::<Result<Vec<_>>>()?;
            let mut columns = Vec::with_capacity(results[0].len());
            while !results[0].is_empty() {
                let mut nesteds = Vec::with_capacity(fields_type.len());
                let mut values = Vec::with_capacity(fields_type.len());
                for result in results.iter_mut() {
                    let (nested, value) = result.pop().unwrap();
                    nesteds.push(nested);
                    values.push(value);
                }
                let array = create_struct(is_nullable, &mut nesteds, values);
                columns.push(array);
            }
            columns.reverse();
            columns
        }
        other => unimplemented!("read datatype {} is not supported", other),
    };
    Ok(result)
}

/// Read all pages of column at once.
pub fn batch_read_column<R: NativeReadBuf>(
    readers: Vec<R>,
    data_type: TableDataType,
    page_metas: Vec<Vec<PageMeta>>,
) -> Result<Column> {
    let results = read_nested_column(readers, data_type, vec![], page_metas)?;
    let columns: Vec<Column> = results.iter().map(|(_, v)| v.clone()).collect();
    let column = Column::concat_columns(columns.into_iter()).unwrap();
    Ok(column)
}
