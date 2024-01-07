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

use parquet2::metadata::ColumnDescriptor;

use crate::arrow::array::Array;
use crate::arrow::array::BinaryArray;
use crate::arrow::array::Utf8Array;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;
use crate::arrow::offset::OffsetsBuffer;
use crate::arrow::types::Offset;
use crate::native::compression::binary::decompress_binary;
use crate::native::read::read_basic::*;
use crate::native::read::BufReader;
use crate::native::read::NativeReadBuf;
use crate::native::read::PageIterator;
use crate::native::PageMeta;

#[derive(Debug)]
pub struct BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, O> BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    fn deserialize(&mut self, num_values: u64, buffer: Vec<u8>) -> Result<Box<dyn Array>> {
        let length = num_values as usize;
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let validity = if self.is_nullable {
            let mut validity_builder = MutableBitmap::with_capacity(length);
            read_validity(&mut reader, length, &mut validity_builder)?;
            Some(std::mem::take(&mut validity_builder).into())
        } else {
            None
        };

        let mut offsets: Vec<O> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);

        decompress_binary(
            &mut reader,
            length,
            &mut offsets,
            &mut values,
            &mut self.scratch,
        )?;

        try_new_binary_array(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets.into()) },
            values.into(),
            validity,
        )
    }
}

impl<I, O> Iterator for BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<Box<dyn Array>>;

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

#[derive(Debug)]
pub struct BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    pub fn new(
        iter: I,
        data_type: DataType,
        leaf: ColumnDescriptor,
        init: Vec<InitNested>,
    ) -> Self {
        Self {
            iter,
            data_type,
            leaf,
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
        let (mut nested, validity) = read_validity_nested(
            &mut reader,
            num_values as usize,
            &self.leaf,
            self.init.clone(),
        )?;
        let length = nested.nested.pop().unwrap().len();

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

pub fn read_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    is_nullable: bool,
    data_type: DataType,
    page_metas: Vec<PageMeta>,
) -> Result<Box<dyn Array>> {
    let num_values = page_metas.iter().map(|p| p.num_values as usize).sum();

    let total_length: usize = page_metas.iter().map(|p| p.length as usize).sum();

    let mut validity_builder = if is_nullable {
        Some(MutableBitmap::with_capacity(num_values))
    } else {
        None
    };
    let mut scratch = vec![];
    let out_off_len = num_values + 2;
    // don't know how much space is needed for the buffer,
    // if not enough, it may need to be reallocated several times.
    let out_buf_len = total_length * 4;
    let mut offsets: Vec<O> = Vec::with_capacity(out_off_len);
    let mut values: Vec<u8> = Vec::with_capacity(out_buf_len);

    for page_meta in page_metas {
        let length = page_meta.num_values as usize;
        if let Some(ref mut validity_builder) = validity_builder {
            read_validity(reader, length, validity_builder)?;
        }

        decompress_binary(reader, length, &mut offsets, &mut values, &mut scratch)?;
    }
    let validity =
        validity_builder.map(|mut validity_builder| std::mem::take(&mut validity_builder).into());
    let offsets: Buffer<O> = offsets.into();
    let values: Buffer<u8> = values.into();

    try_new_binary_array(
        data_type,
        unsafe { OffsetsBuffer::new_unchecked(offsets) },
        values,
        validity,
    )
}

pub fn read_nested_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    let mut scratch = vec![];

    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (mut nested, validity) = read_validity_nested(reader, num_values, &leaf, init.clone())?;
        let length = nested.nested.pop().unwrap().len();

        let mut offsets: Vec<O> = Vec::with_capacity(length + 1);
        let mut values = Vec::with_capacity(0);

        decompress_binary(reader, length, &mut offsets, &mut values, &mut scratch)?;

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
        // todo!("new string")
        let array = Utf8Array::<O>::try_new(data_type, offsets, values, validity)?;
        Ok(Box::new(array) as Box<dyn Array>)
    } else {
        let array = BinaryArray::<O>::try_new(data_type, offsets, values, validity)?;
        Ok(Box::new(array) as Box<dyn Array>)
    }
}
