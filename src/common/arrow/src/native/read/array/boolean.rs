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

use parquet2::metadata::ColumnDescriptor;

use crate::arrow::array::Array;
use crate::arrow::array::BooleanArray;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;
use crate::native::compression::boolean::decompress_boolean;
use crate::native::read::read_basic::*;
use crate::native::read::BufReader;
use crate::native::read::NativeReadBuf;
use crate::native::read::PageIterator;
use crate::native::PageMeta;

#[derive(Debug)]
pub struct BooleanIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
}

impl<I> BooleanIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
            scratch: vec![],
        }
    }
}

impl<I> BooleanIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
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
        let mut bitmap_builder = MutableBitmap::with_capacity(length);

        decompress_boolean(&mut reader, length, &mut bitmap_builder, &mut self.scratch)?;

        let values = std::mem::take(&mut bitmap_builder).into();
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = BooleanArray::try_new(self.data_type.clone(), values, validity)?;
        Ok(Box::new(array) as Box<dyn Array>)
    }
}

impl<I> Iterator for BooleanIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
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
pub struct BooleanNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
}

impl<I> BooleanNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
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
        }
    }
}

impl<I> BooleanNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
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
        let mut bitmap_builder = MutableBitmap::with_capacity(length);

        decompress_boolean(&mut reader, length, &mut bitmap_builder, &mut self.scratch)?;

        let values = std::mem::take(&mut bitmap_builder).into();

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = BooleanArray::try_new(self.data_type.clone(), values, validity)?;
        Ok((nested, Box::new(array) as Box<dyn Array>))
    }
}

impl<I> Iterator for BooleanNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
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

pub fn read_boolean<R: NativeReadBuf>(
    reader: &mut R,
    is_nullable: bool,
    data_type: DataType,
    page_metas: Vec<PageMeta>,
) -> Result<Box<dyn Array>> {
    let num_values = page_metas.iter().map(|p| p.num_values as usize).sum();
    let mut scratch = vec![];
    let mut validity_builder = if is_nullable {
        Some(MutableBitmap::with_capacity(num_values))
    } else {
        None
    };
    let mut bitmap_builder = MutableBitmap::with_capacity(num_values);
    for page_meta in page_metas {
        let length = page_meta.num_values as usize;
        if let Some(ref mut validity_builder) = validity_builder {
            read_validity(reader, length, validity_builder)?;
        }

        decompress_boolean(reader, length, &mut bitmap_builder, &mut scratch)?;
    }
    let validity =
        validity_builder.map(|mut validity_builder| std::mem::take(&mut validity_builder).into());
    let values = std::mem::take(&mut bitmap_builder).into();

    let array = BooleanArray::try_new(data_type, values, validity)?;
    Ok(Box::new(array) as Box<dyn Array>)
}

pub fn read_nested_boolean<R: NativeReadBuf>(
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
        let mut bitmap_builder = MutableBitmap::with_capacity(length);

        decompress_boolean(reader, length, &mut bitmap_builder, &mut scratch)?;

        let values = std::mem::take(&mut bitmap_builder).into();
        let array = BooleanArray::try_new(data_type.clone(), values, validity)?;
        results.push((nested, Box::new(array) as Box<dyn Array>));
    }
    Ok(results)
}
