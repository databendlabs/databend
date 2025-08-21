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

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::FixedLengthEncoding;

use super::null_sentinel;

pub fn encode<T, I>(
    out: &mut BinaryColumnBuilder,
    iter: I,
    (all_null, validity): (bool, Option<&Bitmap>),
    asc: bool,
    nulls_first: bool,
) where
    T: FixedLengthEncoding,
    I: IntoIterator<Item = T>,
{
    if all_null {
        for offset in out.offsets.iter_mut().skip(1) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            out.data[start] = null_sentinel(nulls_first);
            *offset = end as u64;
        }
        return;
    }

    let Some(validity) = validity else {
        for (offset, val) in out.offsets.iter_mut().skip(1).zip(iter) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            let to_write = &mut out.data[start..end];
            to_write[0] = 1;
            let mut encoded = val.encode();
            if !asc {
                // Flip bits to reverse order
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
            }
            to_write[1..].copy_from_slice(encoded.as_ref());
            *offset = end as u64;
        }
        return;
    };

    for ((offset, val), v) in out.offsets.iter_mut().skip(1).zip(iter).zip(validity) {
        let start = *offset as usize;
        let end = start + T::ENCODED_LEN;
        if v {
            let to_write = &mut out.data[start..end];
            to_write[0] = 1;
            let mut encoded = val.encode();
            if !asc {
                // Flip bits to reverse order
                encoded.as_mut().iter_mut().for_each(|v| *v = !*v)
            }
            to_write[1..].copy_from_slice(encoded.as_ref());
        } else {
            out.data[start] = null_sentinel(nulls_first);
        }
        *offset = end as u64;
    }
}
