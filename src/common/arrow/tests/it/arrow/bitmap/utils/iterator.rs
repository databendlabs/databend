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

use databend_common_arrow::arrow::bitmap::utils::BitmapIter;

#[test]
fn basic() {
    let values = &[0b01011011u8];
    let iter = BitmapIter::new(values, 0, 6);
    let result = iter.collect::<Vec<_>>();
    assert_eq!(result, vec![true, true, false, true, true, false])
}

#[test]
fn large() {
    let values = &[0b01011011u8];
    let values = std::iter::repeat(values)
        .take(63)
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let len = 63 * 8;
    let iter = BitmapIter::new(&values, 0, len);
    assert_eq!(iter.count(), len);
}

#[test]
fn offset() {
    let values = &[0b01011011u8];
    let iter = BitmapIter::new(values, 2, 4);
    let result = iter.collect::<Vec<_>>();
    assert_eq!(result, vec![false, true, true, false])
}

#[test]
fn rev() {
    let values = &[0b01011011u8, 0b01011011u8];
    let iter = BitmapIter::new(values, 2, 13);
    let result = iter.rev().collect::<Vec<_>>();
    assert_eq!(
        result,
        vec![
            false, true, true, false, true, false, true, true, false, true, true, false, true
        ]
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
    )
}
