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

use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::encoding::hybrid_rle::Decoder;
use parquet2::encoding::hybrid_rle::HybridEncoded;
use parquet2::encoding::hybrid_rle::HybridRleDecoder;
use parquet2::metadata::ColumnDescriptor;
use parquet2::read::levels::get_bit_width;

use super::NativeReadBuf;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::init_nested;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;

pub fn read_validity<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    builder: &mut MutableBitmap,
) -> Result<()> {
    let mut buf = vec![0u8; 4];
    let def_levels_len = read_u32(reader, buf.as_mut_slice())?;
    if def_levels_len == 0 {
        return Ok(());
    }
    let mut def_levels = vec![0u8; def_levels_len as usize];
    reader.read_exact(def_levels.as_mut_slice())?;

    let decoder = Decoder::new(def_levels.as_slice(), 1);
    for encoded in decoder {
        let encoded = encoded.unwrap();
        match encoded {
            HybridEncoded::Bitpacked(r) => {
                let bitmap_iter = BitmapIter::new(r, 0, length);
                for v in bitmap_iter {
                    unsafe { builder.push_unchecked(v) };
                }
            }
            HybridEncoded::Rle(_, _) => unreachable!(),
        }
    }
    Ok(())
}

pub fn read_validity_nested<R: NativeReadBuf>(
    reader: &mut R,
    num_values: usize,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
) -> Result<(NestedState, Option<Bitmap>)> {
    let mut buf = vec![0u8; 4];
    let additional = read_u32(reader, buf.as_mut_slice())?;
    let rep_levels_len = read_u32(reader, buf.as_mut_slice())?;
    let def_levels_len = read_u32(reader, buf.as_mut_slice())?;
    let max_rep_level = leaf.descriptor.max_rep_level;
    let max_def_level = leaf.descriptor.max_def_level;

    let mut rep_levels = vec![0u8; rep_levels_len as usize];
    reader.read_exact(rep_levels.as_mut_slice())?;
    let mut def_levels = vec![0u8; def_levels_len as usize];
    reader.read_exact(def_levels.as_mut_slice())?;

    let reps = HybridRleDecoder::try_new(&rep_levels, get_bit_width(max_rep_level), num_values)?;
    let defs = HybridRleDecoder::try_new(&def_levels, get_bit_width(max_def_level), num_values)?;
    let mut page_iter = reps.zip(defs).peekable();

    let mut nested = init_nested(&init, num_values);

    // The following code is copied from arrow2 `extend_offsets2` function.
    // https://github.com/jorgecarleitao/arrow2/blob/main/src/io/parquet/read/deserialize/nested_utils.rs#L403
    // The main purpose of this code is to calculate the `NestedState` and `Bitmap`
    // of the nested information by decode `rep_levels` and `def_levels`.
    let max_depth = nested.nested.len();

    let mut cum_sum = vec![0u32; max_depth + 1];
    for (i, nest) in nested.nested.iter().enumerate() {
        let delta = nest.is_nullable() as u32 + nest.is_repeated() as u32;
        cum_sum[i + 1] = cum_sum[i] + delta;
    }

    let mut cum_rep = vec![0u32; max_depth + 1];
    for (i, nest) in nested.nested.iter().enumerate() {
        let delta = nest.is_repeated() as u32;
        cum_rep[i + 1] = cum_rep[i] + delta;
    }

    let mut is_nullable = false;
    let mut builder = MutableBitmap::with_capacity(num_values);

    let mut rows = 0;
    while let Some((rep, def)) = page_iter.next() {
        let rep = rep?;
        let def = def?;
        if rep == 0 {
            rows += 1;
        }

        let mut is_required = false;
        for depth in 0..max_depth {
            let right_level = rep <= cum_rep[depth] && def >= cum_sum[depth];
            if is_required || right_level {
                let length = nested
                    .nested
                    .get(depth + 1)
                    .map(|x| x.len() as i64)
                    // the last depth is the leaf, which is always increased by 1
                    .unwrap_or(1);

                let nest = &mut nested.nested[depth];

                let is_valid = nest.is_nullable() && def > cum_sum[depth];
                nest.push(length, is_valid);
                is_required = nest.is_required() && !is_valid;

                if depth == max_depth - 1 {
                    // the leaf / primitive
                    is_nullable = nest.is_nullable();
                    if is_nullable {
                        let is_valid = (def != cum_sum[depth]) || !nest.is_nullable();
                        if right_level && is_valid {
                            unsafe { builder.push_unchecked(true) };
                        } else {
                            unsafe { builder.push_unchecked(false) };
                        }
                    }
                }
            }
        }

        let next_rep = *page_iter
            .peek()
            .map(|x| x.0.as_ref())
            .transpose()
            .unwrap() // todo: fix this
            .unwrap_or(&0);

        if next_rep == 0 && rows == additional {
            break;
        }
    }

    let validity = if is_nullable {
        Some(std::mem::take(&mut builder).into())
    } else {
        None
    };

    Ok((nested, validity))
}

#[inline(always)]
pub fn read_u32<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u32> {
    r.read_exact(buf)?;
    Ok(u32::from_le_bytes(buf.try_into().unwrap()))
}

pub fn read_compress_header<R: Read>(r: &mut R) -> Result<(u8, usize, usize)> {
    let mut header = vec![0u8; 9];
    r.read_exact(&mut header)?;
    Ok((
        header[0],
        u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize,
        u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize,
    ))
}

#[inline(always)]
pub fn read_u64<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u64> {
    r.read_exact(buf)?;
    Ok(u64::from_le_bytes(buf.try_into().unwrap()))
}
