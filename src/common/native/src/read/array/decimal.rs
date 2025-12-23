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

use std::convert::TryInto;
use std::io::Cursor;
use std::marker::PhantomData;

use databend_common_column::buffer::Buffer;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalSize;

use crate::PageMeta;
use crate::compression::integer::IntegerType;
use crate::compression::integer::decompress_integer;
use crate::error::Result;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::read::BufReader;
use crate::read::NativeReadBuf;
use crate::read::PageIterator;
use crate::read::read_basic::*;

#[derive(Debug)]
pub struct DecimalNestedIter<I, T, F>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
    F: Decimal,
{
    iter: I,
    data_type: TableDataType,
    decimal_size: DecimalSize,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<T>,
    _phantom2: PhantomData<F>,
}

impl<I, T, F> DecimalNestedIter<I, T, F>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
    F: Decimal,
{
    pub fn new(
        iter: I,
        data_type: TableDataType,
        decimal_size: DecimalSize,
        init: Vec<InitNested>,
    ) -> Self {
        Self {
            iter,
            data_type,
            decimal_size,
            init,
            scratch: vec![],
            _phantom: PhantomData,
            _phantom2: PhantomData,
        }
    }
}

impl<I, T, F> DecimalNestedIter<I, T, F>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
    F: Decimal,
    Vec<u8>: TryInto<T::Bytes>,
{
    fn deserialize(&mut self, num_values: u64, buffer: Vec<u8>) -> Result<(NestedState, Column)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) = read_nested(&mut reader, &self.init, num_values as usize)?;
        let length = num_values as usize;

        let mut values = Vec::with_capacity(length);
        decompress_integer(&mut reader, length, &mut values, &mut self.scratch)?;
        assert_eq!(values.len(), length);

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let column: Buffer<T> = values.into();
        let column: Buffer<F> = unsafe { std::mem::transmute(column) };
        let mut col = F::upcast_column(column, self.decimal_size);

        if self.decimal_size.can_carried_by_64() {
            let Column::Decimal(decimal_column) = col else {
                unreachable!()
            };
            col = Column::Decimal(decimal_column.strict_decimal());
        }

        if self.data_type.is_nullable() {
            col = col.wrap_nullable(validity);
        }
        Ok((nested, col))
    }
}

impl<I, T, F> Iterator for DecimalNestedIter<I, T, F>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
    F: Decimal,
    Vec<u8>: TryInto<T::Bytes>,
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

pub(crate) fn read_nested_decimal<T: IntegerType, F: Decimal, R: NativeReadBuf>(
    reader: &mut R,
    data_type: TableDataType,
    decimal_size: DecimalSize,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Column)>> {
    let mut scratch = vec![];
    let mut results = Vec::with_capacity(page_metas.len());
    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (nested, validity) = read_nested(reader, &init, num_values)?;

        let mut values = Vec::with_capacity(num_values);
        decompress_integer(reader, num_values, &mut values, &mut scratch)?;

        let column: Buffer<T> = values.into();
        let column: Buffer<F> = unsafe { std::mem::transmute(column) };
        let mut col = F::upcast_column(column, decimal_size);
        if data_type.is_nullable() {
            col = col.wrap_nullable(validity);
        }
        results.push((nested, col));
    }
    Ok(results)
}
