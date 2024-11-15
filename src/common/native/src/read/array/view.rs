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

use arrow_array::Array;
use arrow_array::BinaryViewArray;
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use byteorder::LittleEndian;
use byteorder::ReadBytesExt;

use crate::arrow::buffer::Buffer;
use crate::error::Result;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::read::read_basic::*;
use crate::read::BufReader;
use crate::read::NativeReadBuf;
use crate::read::PageIterator;
use crate::CommonCompression;
use crate::PageMeta;

#[derive(Debug)]
pub struct ViewArrayNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    data_type: DataType,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
}

impl<I> ViewArrayNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    pub fn new(iter: I, data_type: DataType, init: Vec<InitNested>) -> Self {
        Self {
            iter,
            data_type,
            init,
            scratch: vec![],
        }
    }
}

impl<I> ViewArrayNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    fn deserialize(&mut self, num_values: u64, buffer: Vec<u8>) -> Result<(NestedState, ArrayRef)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) = read_nested(&mut reader, &self.init, num_values as usize)?;
        let length = num_values as usize;

        let array = read_view_array(&mut reader, length, self.data_type.clone(), validity)?;
        Ok((nested, array))
    }
}

impl<I> Iterator for ViewArrayNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    type Item = Result<(NestedState, ArrayRef)>;

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

pub fn read_nested_view_array<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, ArrayRef)>> {
    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (nested, validity) = read_nested(reader, &init, num_values)?;
        let array = read_view_array(reader, num_values, data_type.clone(), validity)?;
        results.push((nested, array));
    }
    Ok(results)
}

fn read_view_array<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    data_type: DataType,
    validity: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let mut scratch = vec![0; 9];
    let (_c, _compressed_size, _uncompressed_size) = read_compress_header(reader, &mut scratch)?;

    let mut buffer = vec![View::default(); length];
    let temp_data =
        unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, length * 16) };
    reader.read_exact(temp_data)?;
    let views = Buffer::from(buffer);

    let buffer_len = reader.read_u32::<LittleEndian>()?;
    let mut buffers = Vec::with_capacity(buffer_len as _);

    for _ in 0..buffer_len {
        scratch.clear();
        let (compression, compressed_size, uncompressed_size) =
            read_compress_header(reader, &mut scratch)?;
        let c = CommonCompression::try_from(&compression)?;
        let mut buffer = vec![];
        c.decompress_common_binary(
            reader,
            uncompressed_size,
            compressed_size,
            &mut buffer,
            &mut scratch,
        )?;
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
        Ok(Arc::new(array))
    }
}
