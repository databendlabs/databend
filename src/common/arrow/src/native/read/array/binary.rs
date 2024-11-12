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
use std::marker::PhantomData;

use crate::arrow::array::Array;
use crate::arrow::array::BinaryArray;
use crate::arrow::array::Utf8Array;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::offset::OffsetsBuffer;
use crate::arrow::types::Offset;
use crate::native::compression::binary::decompress_binary;
use crate::native::nested::InitNested;
use crate::native::nested::NestedState;
use crate::native::read::read_basic::*;
use crate::native::read::BufReader;
use crate::native::read::NativeReadBuf;
use crate::native::read::PageIterator;
use crate::native::PageMeta;

#[derive(Debug)]
pub struct BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    data_type: DataType,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    pub fn new(iter: I, data_type: DataType, init: Vec<InitNested>) -> Self {
        Self {
            iter,
            data_type,
            init,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, O> BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    fn deserialize(
        &mut self,
        num_values: u64,
        buffer: Vec<u8>,
    ) -> Result<(NestedState, Box<dyn Array>)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let length = num_values as usize;
        let (nested, validity) = read_nested(&mut reader, &self.init, num_values as usize)?;
        let mut offsets: Vec<O> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);

        decompress_binary(
            &mut reader,
            length,
            &mut offsets,
            &mut values,
            &mut self.scratch,
        )?;

        let array = try_new_binary_array(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
            values.into(),
            validity,
        )?;
        Ok((nested, array))
    }
}

impl<I, O> Iterator for BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<(NestedState, Box<dyn Array>)>;

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

pub fn read_nested_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    let mut scratch = vec![];

    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (nested, validity) = read_nested(reader, &init, num_values)?;
        let mut offsets: Vec<O> = Vec::with_capacity(num_values + 1);
        let mut values = Vec::with_capacity(0);

        decompress_binary(reader, num_values, &mut offsets, &mut values, &mut scratch)?;

        let array = try_new_binary_array(
            data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
            values.into(),
            validity,
        )?;
        results.push((nested, array));
    }
    Ok(results)
}

fn try_new_binary_array<O: Offset>(
    data_type: DataType,
    offsets: OffsetsBuffer<O>,
    values: Buffer<u8>,
    validity: Option<Bitmap>,
) -> Result<Box<dyn Array>> {
    if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
        let array =
            Utf8Array::<O>::try_new(data_type, offsets, values, validity).map_err(|err| {
                Error::External(
                    "Encountered invalid utf8 data for string type, \
                        if you were reading column with string type from a table, \
                        it's recommended to alter the column type to `BINARY`.\n\
                        Example: `ALTER TABLE <table> MODIFY COLUMN <column> BINARY;`"
                        .to_string(),
                    Box::new(err),
                )
            })?;
        Ok(Box::new(array) as Box<dyn Array>)
    } else {
        let array = BinaryArray::<O>::try_new(data_type, offsets, values, validity)?;
        Ok(Box::new(array) as Box<dyn Array>)
    }
}
