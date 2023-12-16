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

use databend_common_arrow::arrow::types::days_ms;
use databend_common_arrow::arrow::types::months_days_ns;
use databend_common_arrow::arrow::types::BitChunkIter;
use databend_common_arrow::arrow::types::BitChunkOnes;
use databend_common_arrow::arrow::types::NativeType;

#[test]
fn test_basic1() {
    let a = [0b00000001, 0b00010000]; // 0th and 13th entry
    let a = u16::from_ne_bytes(a);
    let iter = BitChunkIter::new(a, 16);
    let r = iter.collect::<Vec<_>>();
    assert_eq!(r, (0..16).map(|x| x == 0 || x == 12).collect::<Vec<_>>(),);
}

#[test]
fn test_ones() {
    let a = [0b00000001, 0b00010000]; // 0th and 13th entry
    let a = u16::from_ne_bytes(a);
    let mut iter = BitChunkOnes::new(a);
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.next(), Some(0));
    assert_eq!(iter.next(), Some(12));
}

#[test]
fn months_days_ns_roundtrip() {
    let a = months_days_ns(1, 2, 3);
    let bytes = a.to_le_bytes();
    assert_eq!(bytes, [1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0]);

    let a = months_days_ns(1, 1, 1);
    assert_eq!(a, months_days_ns::from_be_bytes(a.to_be_bytes()));
}

#[test]
fn days_ms_roundtrip() {
    let a = days_ms(1, 2);
    let bytes = a.to_le_bytes();
    assert_eq!(bytes, [1, 0, 0, 0, 2, 0, 0, 0]);

    let a = days_ms(1, 2);
    assert_eq!(a, days_ms::from_be_bytes(a.to_be_bytes()));
}
