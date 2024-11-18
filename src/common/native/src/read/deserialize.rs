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
use databend_common_expression::TableField;

use super::array::*;
use super::PageIterator;
use crate::error::Result;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::util::n_columns;

/// [`DynIter`] is an iterator adapter adds a custom `nth` method implementation.
pub struct DynIter<'a, V> {
    iter: Box<dyn Iterator<Item = V> + Send + Sync + 'a>,
}

impl<'a, V> Iterator for DynIter<'a, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter.nth(n)
    }
}

impl<'a, V> DynIter<'a, V> {
    pub fn new<I>(iter: I) -> Self
    where I: Iterator<Item = V> + Send + Sync + 'a {
        Self {
            iter: Box::new(iter),
        }
    }
}

pub type ColumnIter<'a> = DynIter<'a, Result<Column>>;

/// [`NestedIter`] is a wrapper iterator used to remove the `NestedState` from inner iterator
/// and return only the `Column`
#[derive(Debug)]
pub struct NestedIter<I>
where I: Iterator<Item = Result<(NestedState, Column)>> + Send + Sync
{
    iter: I,
}

impl<I> NestedIter<I>
where I: Iterator<Item = Result<(NestedState, Column)>> + Send + Sync
{
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I> Iterator for NestedIter<I>
where I: Iterator<Item = Result<(NestedState, Column)>> + Send + Sync
{
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((_, item))) => Some(Ok(item)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((_, item))) => Some(Ok(item)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

pub type NestedIters<'a> = DynIter<'a, Result<(NestedState, Column)>>;

fn deserialize_nested<'a, I>(
    mut readers: Vec<I>,
    data_type: TableDataType,
    mut init: Vec<InitNested>,
) -> Result<NestedIters<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync + 'a,
{
    let is_nullable = matches!(data_type, TableDataType::Nullable(_));
    Ok(match data_type.remove_nullable() {
        TableDataType::Null => unimplemented!(),
        TableDataType::Boolean => {
            init.push(InitNested::Primitive(data_type.is_nullable()));
            DynIter::new(BooleanNestedIter::new(
                readers.pop().unwrap(),
                data_type.clone(),
                init,
            ))
        }
        TableDataType::Number(number) => with_match_integer_double_type!(number,
        |$I| {
            init.push(InitNested::Primitive(data_type.is_nullable()));
            DynIter::new(IntegerNestedIter::<_, $I>::new(
               readers.pop().unwrap(),
                data_type.clone(),
                init,
            ))
        },
        |$T| {
             init.push(InitNested::Primitive(data_type.is_nullable()));
             DynIter::new(DoubleNestedIter::<_, $T>::new(
                readers.pop().unwrap(),
                data_type.clone(),
                init,
            ))
        }
        ),
        TableDataType::Binary => {
            init.push(InitNested::Primitive(data_type.is_nullable()));
            DynIter::new(BinaryNestedIter::<_>::new(
                readers.pop().unwrap(),
                data_type.clone(),
                init,
            ))
        }
        TableDataType::String => {
            init.push(InitNested::Primitive(data_type.is_nullable()));
            DynIter::new(ViewColNestedIter::<_>::new(
                readers.pop().unwrap(),
                data_type.clone(),
                init,
            ))
        }
        TableDataType::Array(inner) => {
            init.push(InitNested::List(inner.is_nullable()));
            let iter = deserialize_nested(readers, inner.as_ref().clone(), init)?;
            DynIter::new(ListIterator::new(iter, inner.as_ref().clone()))
        }
        TableDataType::Map(inner) => {
            init.push(InitNested::List(inner.is_nullable()));
            let iter = deserialize_nested(readers, inner.as_ref().clone(), init)?;
            DynIter::new(MapIterator::new(iter, inner.as_ref().clone()))
        }
        TableDataType::Tuple {
            fields_name: _,
            fields_type,
        } => {
            let columns = fields_type
                .iter()
                .rev()
                .map(|f| {
                    let mut init = init.clone();
                    init.push(InitNested::Struct(data_type.is_nullable()));
                    let n = n_columns(f);
                    let readers = readers.drain(readers.len() - n..).collect();
                    deserialize_nested(readers, f.clone(), init)
                })
                .collect::<Result<Vec<_>>>()?;
            let columns = columns.into_iter().rev().collect();
            DynIter::new(StructIterator::new(
                is_nullable,
                columns,
                fields_type.clone(),
            ))
        }
        _ => unreachable!(),
    })
}

/// An iterator adapter that maps [`PageIterator`]s into an iterator of [`Array`]s.
pub fn column_iter_to_columns<'a, I>(
    readers: Vec<I>,
    field: TableField,
    init: Vec<InitNested>,
) -> Result<ColumnIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync + 'a,
{
    let iter = deserialize_nested(readers, field.data_type().clone(), init)?;
    let nested_iter = NestedIter::new(iter);
    Ok(DynIter::new(nested_iter))
}
