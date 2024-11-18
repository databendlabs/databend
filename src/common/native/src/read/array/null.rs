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

use crate::error::Result;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::read::read_basic::read_nested;
use crate::read::NativeReadBuf;
use crate::read::PageIterator;
use crate::PageMeta;

#[derive(Debug)]
pub struct NullIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    data_type: TableDataType,
}

impl<I> NullIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    pub fn new(iter: I, data_type: TableDataType) -> Self {
        Self { iter, data_type }
    }
}

impl<I> NullIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    fn deserialize(&mut self, num_values: u64) -> Result<Column> {
        let length = num_values as usize;
        Ok(null_column(&self.data_type, length))
    }
}

impl<I> Iterator for NullIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    type Item = Result<Column>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((num_values, mut buffer))) => {
                self.iter.swap_buffer(&mut buffer);
                Some(self.deserialize(num_values))
            }
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((num_values, mut buffer))) => {
                self.iter.swap_buffer(&mut buffer);
                Some(self.deserialize(num_values))
            }
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }
}

pub fn read_nested_null<R: NativeReadBuf>(
    reader: &mut R,
    data_type: &TableDataType,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Column)>> {
    let mut results = Vec::with_capacity(page_metas.len());
    for page_meta in page_metas {
        let length = page_meta.num_values as usize;
        let (nested, _) = read_nested(reader, &init, length)?;

        let col = null_column(data_type, length);
        results.push((nested, col));
    }
    Ok(results)
}

fn null_column(data_type: &TableDataType, length: usize) -> Column {
    match data_type {
        TableDataType::Null => Column::Null { len: length },
        TableDataType::EmptyArray => Column::EmptyArray { len: length },
        TableDataType::EmptyMap => Column::EmptyMap { len: length },
        _ => unreachable!(),
    }
}
