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

use std::io::Cursor;

use databend_common_column::binary::BinaryColumn;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::GeographyColumn;

use crate::PageMeta;
use crate::compression::binary::decompress_binary;
use crate::error::Result;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::read::BufReader;
use crate::read::NativeReadBuf;
use crate::read::PageIterator;
use crate::read::read_basic::*;

#[derive(Debug)]
pub struct BinaryNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    data_type: TableDataType,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
}

impl<I> BinaryNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    pub fn new(iter: I, data_type: TableDataType, init: Vec<InitNested>) -> Self {
        Self {
            iter,
            data_type,
            init,
            scratch: vec![],
        }
    }
}

impl<I> BinaryNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    fn deserialize(&mut self, num_values: u64, buffer: Vec<u8>) -> Result<(NestedState, Column)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let length = num_values as usize;

        let (nested, validity) = read_nested(&mut reader, &self.init, num_values as usize)?;
        let mut offsets: Vec<u64> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);

        decompress_binary(
            &mut reader,
            length,
            &mut offsets,
            &mut values,
            &mut self.scratch,
        )?;

        let column =
            try_new_binary_column(&self.data_type, offsets.into(), values.into(), validity)?;
        Ok((nested, column))
    }
}

impl<I> Iterator for BinaryNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    type Item = Result<(NestedState, Column)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }
}

pub fn read_nested_binary<R: NativeReadBuf>(
    reader: &mut R,
    data_type: TableDataType,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Column)>> {
    let mut scratch = vec![];

    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (nested, validity) = read_nested(reader, &init, num_values)?;
        let mut offsets: Vec<u64> = Vec::with_capacity(num_values + 1);
        let mut values = Vec::with_capacity(0);

        decompress_binary(reader, num_values, &mut offsets, &mut values, &mut scratch)?;

        let column = try_new_binary_column(&data_type, offsets.into(), values.into(), validity)?;
        results.push((nested, column));
    }
    Ok(results)
}

fn try_new_binary_column(
    data_type: &TableDataType,
    offsets: Buffer<u64>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
) -> Result<Column> {
    let column = BinaryColumn::new(values, offsets);
    Ok(binary_column_to_column(data_type, column, validity))
}

fn binary_column_to_column(
    data_type: &TableDataType,
    column: BinaryColumn,
    validity: Option<Bitmap>,
) -> Column {
    let col = match data_type.remove_nullable() {
        TableDataType::Binary => Column::Binary(column),
        TableDataType::Bitmap => Column::Bitmap(column),
        TableDataType::Variant => Column::Variant(column),
        TableDataType::Geometry => Column::Geometry(column),
        TableDataType::Geography => Column::Geography(GeographyColumn(column)),
        _ => unreachable!(),
    };
    if data_type.is_nullable() {
        col.wrap_nullable(validity)
    } else {
        col
    }
}
