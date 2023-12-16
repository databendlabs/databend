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

use databend_common_arrow::arrow::bitmap::utils::BitChunksExact;

#[test]
fn basics() {
    let mut iter = BitChunksExact::<u8>::new(&[0b11111111u8, 0b00000001u8], 9);
    assert_eq!(iter.next().unwrap(), 0b11111111u8);
    assert_eq!(iter.remainder(), 0b00000001u8);
}

#[test]
fn basics_u16_small() {
    let mut iter = BitChunksExact::<u16>::new(&[0b11111111u8], 7);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.remainder(), 0b0000_0000_1111_1111u16);
}

#[test]
fn basics_u16() {
    let mut iter = BitChunksExact::<u16>::new(&[0b11111111u8, 0b00000001u8], 9);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.remainder(), 0b0000_0001_1111_1111u16);
}

#[test]
fn remainder_u16() {
    let mut iter = BitChunksExact::<u16>::new(
        &[0b11111111u8, 0b00000001u8, 0b00000001u8, 0b11011011u8],
        23,
    );
    assert_eq!(iter.next(), Some(511));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.remainder(), 1u16);
}
