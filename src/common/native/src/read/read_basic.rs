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
use std::io::Read;

use databend_common_expression::types::Bitmap;

use super::NativeReadBuf;
use crate::compression::Compression;
use crate::error::Result;
use crate::nested::FixedListNested;
use crate::nested::InitNested;
use crate::nested::ListNested;
use crate::nested::Nested;

pub fn read_validity<R: NativeReadBuf>(reader: &mut R) -> Result<Option<Bitmap>> {
    let mut buf = vec![0u8; 4];
    let length = read_u32(reader, &mut buf)? as usize;
    if length > 0 {
        buf.resize(length.div_ceil(8), 0);
        reader.read_exact(&mut buf)?;
        Ok(Some(Bitmap::try_new(buf, length)?))
    } else {
        Ok(None)
    }
}

// Read nested from reader and pop the leaf nested
pub fn read_nested<R: NativeReadBuf>(
    reader: &mut R,
    init: &[InitNested],
    leaf_length: usize,
) -> Result<(Vec<Nested>, Option<Bitmap>)> {
    assert!(!init.is_empty());
    let is_simple_nested = init.len() == 1;

    if is_simple_nested {
        let n = init[0];
        let bitmap = if n.is_nullable() {
            read_validity(reader)?
        } else {
            None
        };

        Ok((vec![], bitmap))
    } else {
        let mut results = Vec::with_capacity(init.len());
        for n in init {
            let bitmap = if n.is_nullable() {
                read_validity(reader)?
            } else {
                None
            };

            match n {
                InitNested::Primitive(_) => {
                    results.push(Nested::Primitive(leaf_length, n.is_nullable(), bitmap))
                }
                InitNested::List(_) => {
                    let mut buf = vec![0u8; 4];
                    let length = read_u32(reader, &mut buf)?;
                    let mut values = vec![0u64; length as usize];
                    let bytes: &mut [u8] = bytemuck::cast_slice_mut(values.as_mut());
                    reader.read_exact(bytes)?;

                    results.push(Nested::LargeList(ListNested::new(
                        values.into(),
                        bitmap,
                        n.is_nullable(),
                    )))
                }
                InitNested::FixedList(_) => {
                    let mut buf = vec![0u8; 4];
                    let length = read_u32(reader, &mut buf)?;
                    let dimension = read_u32(reader, &mut buf)?;
                    results.push(Nested::FixedList(FixedListNested::new(
                        dimension as usize,
                        length as usize,
                        bitmap,
                        n.is_nullable(),
                    )))
                }
                InitNested::Struct(_) => {
                    results.push(Nested::Struct(leaf_length, n.is_nullable(), bitmap))
                }
            }
        }
        let bitmap = results.pop().unwrap().validity().clone();
        Ok((results, bitmap))
    }
}

#[inline(always)]
pub fn read_u32<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u32> {
    r.read_exact(buf)?;
    Ok(u32::from_le_bytes(buf.try_into().unwrap()))
}

pub fn read_compress_header<R: Read>(
    r: &mut R,
    scratch: &mut Vec<u8>,
) -> Result<(Compression, usize, usize)> {
    scratch.reserve(9);
    let temp_data = unsafe { std::slice::from_raw_parts_mut(scratch.as_mut_ptr(), 9) };
    r.read_exact(temp_data)?;
    Ok((
        Compression::from_codec(temp_data[0])?,
        u32::from_le_bytes(temp_data[1..5].try_into().unwrap()) as usize,
        u32::from_le_bytes(temp_data[5..9].try_into().unwrap()) as usize,
    ))
}

#[inline(always)]
pub fn read_u64<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u64> {
    r.read_exact(buf)?;
    Ok(u64::from_le_bytes(buf.try_into().unwrap()))
}
