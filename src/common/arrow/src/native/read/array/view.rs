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

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use parquet2::metadata::ColumnDescriptor;

use crate::arrow::array::Array;
use crate::arrow::array::BinaryViewArray;
use crate::arrow::array::View;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::buffer::Buffer;
use crate::arrow::compute::concatenate::concatenate;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;
use crate::native::read::read_basic::*;
use crate::native::read::BufReader;
use crate::native::read::NativeReadBuf;
use crate::native::read::PageIterator;
use crate::native::PageMeta;

#[derive(Debug)]
pub struct ViewArrayIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
}

impl<I> ViewArrayIter<I>
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

impl<I> ViewArrayIter<I>
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

        read_view_array(&mut reader, length, self.data_type.clone(), validity)
    }
}

impl<I> Iterator for ViewArrayIter<I>
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
pub struct ViewArrayNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
}

impl<I> ViewArrayNestedIter<I>
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

impl<I> ViewArrayNestedIter<I>
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
        let array = read_view_array(&mut reader, length, self.data_type.clone(), validity)?;
        Ok((nested, array))
    }
}

impl<I> Iterator for ViewArrayNestedIter<I>
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

pub fn read_view<R: NativeReadBuf>(
    reader: &mut R,
    is_nullable: bool,
    data_type: DataType,
    page_metas: Vec<PageMeta>,
) -> Result<Box<dyn Array>> {
    let num_values = page_metas.iter().map(|p| p.num_values as usize).sum();
    let mut validity_builder = if is_nullable {
        Some(MutableBitmap::with_capacity(num_values))
    } else {
        None
    };
    let mut arrays = vec![];
    for page_meta in page_metas {
        let length = page_meta.num_values as usize;
        if let Some(ref mut validity_builder) = validity_builder {
            read_validity(reader, length, validity_builder)?;
        }
        let array = read_view_array(reader, length, data_type.clone(), None)?;
        arrays.push(array);
    }

    let validity =
        validity_builder.map(|mut validity_builder| std::mem::take(&mut validity_builder).into());
    let arrays = arrays.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
    let array = concatenate(&arrays)?;
    let array = array.with_validity(validity);
    Ok(array)
}

pub fn read_nested_view_array<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (mut nested, validity) = read_validity_nested(reader, num_values, &leaf, init.clone())?;
        let length = nested.nested.pop().unwrap().len();

        let array = read_view_array(reader, length, data_type.clone(), validity)?;
        results.push((nested, array));
    }
    Ok(results)
}

fn read_view_array<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    data_type: DataType,
    validity: Option<Bitmap>,
) -> Result<Box<dyn Array>> {
    let mut buffer = vec![View::default(); length];
    let temp_data =
        unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, length * 16) };
    reader.read_exact(temp_data)?;
    let views = Buffer::from(buffer);

    let buffer_len = reader.read_u32::<LittleEndian>()?;
    let mut buffers = Vec::with_capacity(buffer_len as _);

    for _ in 0..buffer_len {
        let len = reader.read_u32::<LittleEndian>()?;
        let mut buffer = vec![0u8; len as usize];
        reader.read_exact(&mut buffer)?;
        buffers.push(Buffer::from(buffer));
    }

    let array = unsafe {
        BinaryViewArray::new_unchecked_unknown_md(
            data_type.clone(),
            views,
            buffers.into(),
            validity,
            None,
        )
    };

    if matches!(data_type, DataType::Utf8View) {
        Ok(Box::new(array.to_utf8view()?))
    } else {
        Ok(Box::new(array))
    }
}
