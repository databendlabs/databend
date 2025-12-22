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
use databend_common_expression::ColumnViewIter;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::types::AccessType;

use super::null_sentinel;

pub(super) fn fixed_encode<T, I>(
    data: &mut [u8],
    offsets: &mut [u64],
    iter: I,
    (all_null, validity): (bool, Option<&Bitmap>),
    asc: bool,
    nulls_first: bool,
) where
    T: FixedLengthEncoding,
    I: IntoIterator<Item = T>,
{
    fixed_encode_with_fn(
        data,
        offsets,
        iter,
        (all_null, validity),
        |value, to_write| {
            match value {
                Some(val) => {
                    // Logic for encoding a non-null value
                    to_write[0] = 1;
                    let mut encoded = val.encode();
                    if !asc {
                        // Flip bits to reverse order
                        encoded.as_mut().iter_mut().for_each(|v| *v = !*v);
                    }
                    to_write[1..].copy_from_slice(encoded.as_ref());
                }
                None => {
                    // Logic for encoding a null value
                    to_write[0] = null_sentinel(nulls_first);
                }
            }
        },
    );
}

pub(super) fn fixed_encode_const<'a, A>(
    data: &mut [u8],
    offsets: &mut [u64],
    is_null: bool,
    scalar: A::ScalarRef<'a>,
    asc: bool,
    nulls_first: bool,
) where
    A: AccessType,
    A::ScalarRef<'a>: FixedLengthEncoding,
{
    fixed_encode::<A::ScalarRef<'a>, _>(
        data,
        offsets,
        ColumnViewIter::<A>::Const(scalar, offsets.len() - 1),
        (is_null, None),
        asc,
        nulls_first,
    );
}

fn fixed_encode_with_fn<T, I, F>(
    data: &mut [u8],
    offsets: &mut [u64],
    iter: I,
    (all_null, validity): (bool, Option<&Bitmap>),
    mut encode_fn: F,
) where
    T: FixedLengthEncoding,
    I: IntoIterator<Item = T>,
    F: FnMut(Option<T>, &mut [u8]),
{
    if all_null {
        for offset in offsets.iter_mut().skip(1) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            encode_fn(None, &mut data[start..end]);
            *offset = end as u64;
        }
        return;
    }

    let Some(validity) = validity else {
        for (offset, val) in offsets.iter_mut().skip(1).zip(iter) {
            let start = *offset as usize;
            let end = start + T::ENCODED_LEN;
            encode_fn(Some(val), &mut data[start..end]);
            *offset = end as u64;
        }
        return;
    };

    for ((offset, val), v) in offsets.iter_mut().skip(1).zip(iter).zip(validity) {
        let start = *offset as usize;
        let end = start + T::ENCODED_LEN;
        encode_fn(v.then_some(val), &mut data[start..end]);
        *offset = end as u64;
    }
}
