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

use databend_common_arrow::arrow::bitmap::utils::*;
use proptest::prelude::*;

use crate::arrow::bitmap::bitmap_strategy;

mod bit_chunks_exact;
mod chunk_iter;
mod fmt;
mod iterator;
mod slice_iterator;
mod zip_validity;

#[test]
fn get_bit_basics() {
    let input: &[u8] = &[
        0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
        0b01000000, 0b11111111,
    ];
    for i in 0..8 {
        assert!(!get_bit(input, i));
    }
    assert!(get_bit(input, 8));
    for i in 8 + 1..2 * 8 {
        assert!(!get_bit(input, i));
    }
    assert!(get_bit(input, 2 * 8 + 1));
    for i in 2 * 8 + 2..3 * 8 {
        assert!(!get_bit(input, i));
    }
    assert!(get_bit(input, 3 * 8 + 2));
    for i in 3 * 8 + 3..4 * 8 {
        assert!(!get_bit(input, i));
    }
    assert!(get_bit(input, 4 * 8 + 3));
}

#[test]
fn count_zeros_basics() {
    let input: &[u8] = &[
        0b01001001, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
        0b01000000, 0b11111111,
    ];
    assert_eq!(count_zeros(input, 0, 8), 8 - 3);
    assert_eq!(count_zeros(input, 1, 7), 7 - 2);
    assert_eq!(count_zeros(input, 1, 8), 8 - 3);
    assert_eq!(count_zeros(input, 2, 7), 7 - 3);
    assert_eq!(count_zeros(input, 0, 32), 32 - 6);
    assert_eq!(count_zeros(input, 9, 2), 2);

    let input: &[u8] = &[0b01000000, 0b01000001];
    assert_eq!(count_zeros(input, 8, 2), 1);
    assert_eq!(count_zeros(input, 8, 3), 2);
    assert_eq!(count_zeros(input, 8, 4), 3);
    assert_eq!(count_zeros(input, 8, 5), 4);
    assert_eq!(count_zeros(input, 8, 6), 5);
    assert_eq!(count_zeros(input, 8, 7), 5);
    assert_eq!(count_zeros(input, 8, 8), 6);

    let input: &[u8] = &[0b01000000, 0b01010101];
    assert_eq!(count_zeros(input, 9, 2), 1);
    assert_eq!(count_zeros(input, 10, 2), 1);
    assert_eq!(count_zeros(input, 11, 2), 1);
    assert_eq!(count_zeros(input, 12, 2), 1);
    assert_eq!(count_zeros(input, 13, 2), 1);
    assert_eq!(count_zeros(input, 14, 2), 1);
}

#[test]
fn count_zeros_1() {
    // offset = 10, len = 90 => remainder
    let input: &[u8] = &[73, 146, 36, 73, 146, 36, 73, 146, 36, 73, 146, 36, 9];
    assert_eq!(count_zeros(input, 10, 90), 60);
}

proptest! {
    /// Asserts that `Bitmap::null_count` equals the number of unset bits
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn null_count(bitmap in bitmap_strategy()) {
        let sum_of_sets: usize = (0..bitmap.len()).map(|x| (!bitmap.get_bit(x)) as usize).sum();
        assert_eq!(bitmap.unset_bits(), sum_of_sets);
    }
}
