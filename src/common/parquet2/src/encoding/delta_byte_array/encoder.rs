// Copyright [2021] [Jorge C Leitao]
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

use super::super::delta_bitpacked;
use crate::encoding::delta_length_byte_array;

/// Encodes an iterator of according to DELTA_BYTE_ARRAY
pub fn encode<'a, I: Iterator<Item = &'a [u8]> + Clone>(iterator: I, buffer: &mut Vec<u8>) {
    let mut previous = b"".as_ref();

    let mut sum_lengths = 0;
    let prefixes = iterator
        .clone()
        .map(|item| {
            let prefix_length = item
                .iter()
                .zip(previous.iter())
                .enumerate()
                // find first difference
                .find_map(|(length, (lhs, rhs))| (lhs != rhs).then_some(length))
                .unwrap_or(previous.len());
            previous = item;

            sum_lengths += item.len() - prefix_length;
            prefix_length as i64
        })
        .collect::<Vec<_>>();
    delta_bitpacked::encode(prefixes.iter().copied(), buffer);

    let remaining = iterator
        .zip(prefixes)
        .map(|(item, prefix)| &item[prefix as usize..]);

    delta_length_byte_array::encode(remaining, buffer);
}
