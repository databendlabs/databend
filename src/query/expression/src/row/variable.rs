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

use databend_common_arrow::arrow::bitmap::Bitmap;

use super::row_converter::null_sentinel;
use crate::types::string::StringColumnBuilder;

/// The block size of the variable length encoding
const BLOCK_SIZE: usize = 32;

/// The continuation token
const BLOCK_CONTINUATION: u8 = 0xFF;

/// Indicates an empty string
const EMPTY_SENTINEL: u8 = 1;

/// Indicates a non-empty string
const NON_EMPTY_SENTINEL: u8 = 2;

/// Returns the length of the encoded representation of a byte array, including the null byte
#[inline]
pub(super) fn encoded_len(val: &[u8], is_null: bool) -> usize {
    if is_null {
        1
    } else {
        1 + val.len().div_ceil(BLOCK_SIZE) * (BLOCK_SIZE + 1)
    }
}

/// Variable length values are encoded as
///
/// - single `0_u8` if null
/// - single `1_u8` if empty array
/// - `2_u8` if not empty, followed by one or more blocks
///
/// where a block is encoded as
///
/// - [`BLOCK_SIZE`] bytes of string data, padded with 0s
/// - `0xFF_u8` if this is not the last block for this string
/// - otherwise the length of the block as a `u8`
pub(super) fn encode<'a, I>(
    out: &mut StringColumnBuilder,
    i: I,
    (all_null, validity): (bool, Option<&Bitmap>),
    asc: bool,
    nulls_first: bool,
) where
    I: Iterator<Item = &'a [u8]>,
{
    if let Some(validity) = validity {
        for ((offset, val), v) in out.offsets.iter_mut().skip(1).zip(i).zip(validity.iter()) {
            *offset += encode_one(&mut out.data[*offset as usize..], val, !v, asc, nulls_first);
        }
    } else if all_null {
        for (offset, val) in out.offsets.iter_mut().skip(1).zip(i) {
            *offset += encode_one(
                &mut out.data[*offset as usize..],
                val,
                true,
                asc,
                nulls_first,
            );
        }
    } else {
        for (offset, val) in out.offsets.iter_mut().skip(1).zip(i) {
            *offset += encode_one(
                &mut out.data[*offset as usize..],
                val,
                false,
                asc,
                nulls_first,
            );
        }
    }
}

fn encode_one(out: &mut [u8], val: &[u8], is_null: bool, asc: bool, nulls_first: bool) -> u64 {
    if is_null {
        out[0] = null_sentinel(nulls_first);
        1
    } else if val.is_empty() {
        out[0] = if asc { EMPTY_SENTINEL } else { !EMPTY_SENTINEL };
        1
    } else {
        let block_count = val.len().div_ceil(BLOCK_SIZE);
        let end_offset = 1 + block_count * (BLOCK_SIZE + 1);
        let to_write = &mut out[..end_offset];

        // Write `2_u8` to demarcate as non-empty, non-null string
        to_write[0] = NON_EMPTY_SENTINEL;

        let chunks = val.chunks_exact(BLOCK_SIZE);
        let remainder = chunks.remainder();
        for (input, output) in chunks
            .clone()
            .zip(to_write[1..].chunks_exact_mut(BLOCK_SIZE + 1))
        {
            let input: &[u8; BLOCK_SIZE] = input.try_into().unwrap();
            let out_block: &mut [u8; BLOCK_SIZE] = (&mut output[..BLOCK_SIZE]).try_into().unwrap();

            *out_block = *input;

            // Indicate that there are further blocks to follow
            output[BLOCK_SIZE] = BLOCK_CONTINUATION;
        }

        if !remainder.is_empty() {
            let start_offset = 1 + (block_count - 1) * (BLOCK_SIZE + 1);
            to_write[start_offset..start_offset + remainder.len()].copy_from_slice(remainder);
            *to_write.last_mut().unwrap() = remainder.len() as u8;
        } else {
            // We must overwrite the continuation marker written by the loop above
            *to_write.last_mut().unwrap() = BLOCK_SIZE as u8;
        }

        if !asc {
            // Invert bits
            to_write.iter_mut().for_each(|v| *v = !*v)
        }
        end_offset as u64
    }
}
