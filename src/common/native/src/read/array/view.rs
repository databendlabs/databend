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
use databend_common_column::binview::Utf8ViewColumn;
use databend_common_column::binview::View;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;

use crate::CommonCompression;
use crate::PageMeta;
use crate::compression::integer::decompress_integer;
use crate::error::Result;
use crate::nested::InitNested;
use crate::nested::NestedState;
use crate::read::BufReader;
use crate::read::NativeReadBuf;
use crate::read::PageIterator;
use crate::read::read_basic::*;

#[derive(Debug)]
pub struct ViewColNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    iter: I,
    data_type: TableDataType,
    init: Vec<InitNested>,
}

impl<I> ViewColNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    pub fn new(iter: I, data_type: TableDataType, init: Vec<InitNested>) -> Self {
        Self {
            iter,
            data_type,
            init,
        }
    }
}

impl<I> ViewColNestedIter<I>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync
{
    fn deserialize(&mut self, num_values: u64, buffer: Vec<u8>) -> Result<(NestedState, Column)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) = read_nested(&mut reader, &self.init, num_values as usize)?;
        let length = num_values as usize;

        let col = read_view_col(&mut reader, length, self.data_type.clone(), validity)?;
        Ok((nested, col))
    }
}

impl<I> Iterator for ViewColNestedIter<I>
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

pub fn read_nested_view_col<R: NativeReadBuf>(
    reader: &mut R,
    data_type: TableDataType,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Column)>> {
    let mut results = Vec::with_capacity(page_metas.len());

    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (nested, validity) = read_nested(reader, &init, num_values)?;
        let col = read_view_col(reader, num_values, data_type.clone(), validity)?;
        results.push((nested, col));
    }
    Ok(results)
}

fn read_view_col<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    data_type: TableDataType,
    validity: Option<Bitmap>,
) -> Result<Column> {
    let mut scratch = Vec::new();
    let mut views: Vec<i128> = Vec::with_capacity(length);
    decompress_integer(reader, length, &mut views, &mut scratch)?;

    let views: Buffer<i128> = views.into();
    let views = unsafe { std::mem::transmute::<Buffer<i128>, Buffer<View>>(views) };

    let buffer_len = reader.read_u32::<LittleEndian>()?;
    let mut buffers = Vec::with_capacity(buffer_len as usize);

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

    let col = unsafe {
        Column::String(Utf8ViewColumn::new_unchecked_unknown_md(
            views,
            buffers.into(),
            None,
        ))
    };

    if data_type.is_nullable() {
        Ok(col.wrap_nullable(validity))
    } else {
        Ok(col)
    }
}
