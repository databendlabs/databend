// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::MutableBuffer;

use crate::prelude::*;

pub fn transform<F>(from: &DFStringArray, estimate_bytes: usize, mut f: F) -> DFStringArray
where F: FnMut(&[u8], &mut [u8]) -> (usize, bool) {
    let mut values: MutableBuffer<u8> = MutableBuffer::with_capacity(estimate_bytes);
    let mut offsets: MutableBuffer<i64> = MutableBuffer::with_capacity(from.len() + 1);
    let mut validity = MutableBitmap::with_capacity(from.len());
    offsets.push(0);

    let mut offset: usize = 0;

    unsafe {
        for x in from.into_no_null_iter() {
            let bytes =
                std::slice::from_raw_parts_mut(values.as_mut_ptr(), values.capacity() - offset);
            let (len, is_null) = f(x, bytes);

            offset += len;
            offsets.push(i64::from_isize(offset as isize).unwrap());
            if is_null {
                validity.push(false);
            } else {
                validity.push(true);
            }
        }
        values.set_len(offset);
        values.shrink_to_fit();
        let validity = combine_validities(from.array.validity(), Some(&validity.into()));
        let array = BinaryArray::<i64>::from_data_unchecked(
            BinaryArray::<i64>::default_data_type(),
            offsets.into(),
            values.into(),
            validity,
        );
        DFStringArray::from_arrow_array(&array)
    }
}
