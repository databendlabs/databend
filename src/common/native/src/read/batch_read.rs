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

use databend_common_expression::Column;
use databend_common_expression::TableDataType;

use super::array::*;
use super::NativeReadBuf;
use crate::error::Result;
use crate::nested::create_list;
use crate::nested::create_map;
use crate::nested::create_struct;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::util::n_columns;
use crate::PageMeta;

pub fn read_nested<R: NativeReadBuf>(
    mut readers: Vec<R>,
    data_type: TableDataType,
    mut init: Vec<InitNested>,
    mut page_metas: Vec<Vec<PageMeta>>,
) -> Result<Vec<(NestedState, Column)>> {
    let is_nullable = data_type.is_nullable();
    use TableDataType::*;
    let result = match data_type.remove_nullable() {
        Null => unimplemented!("null"),
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
            read_nested_integer::<$T, _>(
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
        Decimal(_) => todo!(),
        Binary => {
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
            let results = read_nested(readers, inner.as_ref().clone(), init, page_metas)?;
            let mut columns = Vec::with_capacity(results.len());
            for (mut nested, values) in results {
                let array = create_list(data_type.clone(), &mut nested, values);
                columns.push((nested, array));
            }
            columns
        }
        Map(inner) => {
            init.push(InitNested::List(is_nullable));
            let results = read_nested(readers, inner.as_ref().clone(), init, page_metas)?;
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
                    let n = n_columns(&data_type);
                    let readers = readers.drain(..n).collect();
                    let page_metas = page_metas.drain(..n).collect();
                    read_nested(readers, f.clone(), init, page_metas)
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
        _ => todo!("xxx"),
    };
    Ok(result)
}

/// Read all pages of column at once.
pub fn batch_read_column<R: NativeReadBuf>(
    readers: Vec<R>,
    data_type: TableDataType,
    page_metas: Vec<Vec<PageMeta>>,
) -> Result<Column> {
    let results = read_nested(readers, data_type, vec![], page_metas)?;
    let columns: Vec<Column> = results.iter().map(|(_, v)| v.clone()).collect();
    let column = Column::concat_columns(columns.into_iter()).unwrap();
    Ok(column)
}
