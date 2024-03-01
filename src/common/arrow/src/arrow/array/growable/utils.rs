// Copyright 2020-2022 Jorge C. Leitão
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

use crate::arrow::array::Array;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::offset::Offset;

// function used to extend nulls from arrays. This function's lifetime is bound to the array
// because it reads nulls from it.
pub(super) type ExtendNullBits<'a> = Box<dyn Fn(&mut MutableBitmap, usize, usize) + 'a>;

pub(super) fn build_extend_null_bits(array: &dyn Array, use_validity: bool) -> ExtendNullBits {
    if let Some(bitmap) = array.validity() {
        Box::new(move |validity, start, len| {
            debug_assert!(start + len <= bitmap.len());
            let (slice, offset, _) = bitmap.as_slice();
            // safety: invariant offset + length <= slice.len()
            unsafe {
                validity.extend_from_slice_unchecked(slice, start + offset, len);
            }
        })
    } else if use_validity {
        Box::new(|validity, _, len| {
            validity.extend_constant(len, true);
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

pub(super) fn prepare_validity(use_validity: bool, capacity: usize) -> Option<MutableBitmap> {
    if use_validity {
        Some(MutableBitmap::with_capacity(capacity))
    } else {
        None
    }
}

#[inline]
pub(super) fn extend_offset_values<O: Offset>(
    buffer: &mut Vec<u8>,
    offsets: &[O],
    values: &[u8],
    start: usize,
    len: usize,
) {
    let start_values = offsets[start].to_usize();
    let end_values = offsets[start + len].to_usize();
    let new_values = &values[start_values..end_values];
    buffer.extend_from_slice(new_values);
}

pub(super) fn extend_validity(
    mutable_validity: &mut Option<MutableBitmap>,
    array: &dyn Array,
    start: usize,
    len: usize,
) {
    if let Some(mutable_validity) = mutable_validity {
        match array.validity() {
            None => mutable_validity.extend_constant(len, true),
            Some(validity) => {
                debug_assert!(start + len <= validity.len());
                let (slice, offset, _) = validity.as_slice();
                // safety: invariant offset + length <= slice.len()
                unsafe {
                    mutable_validity.extend_from_slice_unchecked(slice, start + offset, len);
                }
            }
        }
    }
}
