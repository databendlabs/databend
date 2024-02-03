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

use databend_common_base::containers::HyperLogLog;

const P: usize = 14;
const NUM_REGISTERS: usize = 1 << P;

fn compare_with_delta(got: usize, expected: usize) {
    let expected = expected as f64;
    let diff = (got as f64) - expected;
    let diff = diff.abs() / expected;
    // times 6 because we want the tests to be stable
    // so we allow a rather large margin of error
    // this is adopted from redis's unit test version as well
    let margin = 1.04 / ((NUM_REGISTERS as f64).sqrt()) * 6.0;
    assert!(
        diff <= margin,
        "{} is not near {} percent of {} which is ({}, {})",
        got,
        margin,
        expected,
        expected * (1.0 - margin),
        expected * (1.0 + margin)
    );
}

macro_rules! sized_number_test {
    ($SIZE: expr, $T: tt) => {{
        let mut hll = HyperLogLog::<P>::new();
        for i in 0..$SIZE {
            hll.add_object(&(i as $T));
        }
        compare_with_delta(hll.count(), $SIZE);
    }};
}

macro_rules! typed_large_number_test {
    ($SIZE: expr) => {{
        sized_number_test!($SIZE, u64);
        sized_number_test!($SIZE, u128);
        sized_number_test!($SIZE, i64);
        sized_number_test!($SIZE, i128);
    }};
}

macro_rules! typed_number_test {
    ($SIZE: expr) => {{
        sized_number_test!($SIZE, u16);
        sized_number_test!($SIZE, u32);
        sized_number_test!($SIZE, i16);
        sized_number_test!($SIZE, i32);
        typed_large_number_test!($SIZE);
    }};
}

#[test]
fn test_empty() {
    let hll = HyperLogLog::<P>::new();
    assert_eq!(hll.count(), 0);
}

#[test]
fn test_one() {
    let mut hll = HyperLogLog::<P>::new();
    hll.add_hash(1);
    assert_eq!(hll.count(), 1);
}

#[test]
fn test_number_100() {
    typed_number_test!(100);
}

#[test]
fn test_number_1k() {
    typed_number_test!(1_000);
}

#[test]
fn test_number_10k() {
    typed_number_test!(10_000);
}

#[test]
fn test_number_100k() {
    typed_large_number_test!(100_000);
}

#[test]
fn test_number_1m() {
    typed_large_number_test!(1_000_000);
}

#[test]
fn test_empty_merge() {
    let mut hll = HyperLogLog::<P>::new();
    hll.merge(&HyperLogLog::<P>::new());
    assert_eq!(hll.count(), 0);
}

#[test]
fn test_merge_overlapped() {
    let mut hll = HyperLogLog::<P>::new();
    for i in 0..1000 {
        hll.add_object(&i);
    }

    let other = HyperLogLog::<P>::new();
    for i in 0..1000 {
        hll.add_object(&i);
    }

    hll.merge(&other);
    compare_with_delta(hll.count(), 1000);
}

#[test]
fn test_repetition() {
    let mut hll = HyperLogLog::<P>::new();
    for i in 0..1_000_000 {
        hll.add_object(&(i % 1000));
    }
    compare_with_delta(hll.count(), 1000);
}

#[test]
fn test_serde() {
    let mut hll = HyperLogLog::<P>::new();
    json_serde_equal(&hll);

    for i in 0..100000 {
        hll.add_object(&(i % 200));
    }
    json_serde_equal(&hll);

    let hll = HyperLogLog::<P>::with_registers(vec![1; 1 << P]);
    json_serde_equal(&hll);
}

fn json_serde_equal<T>(t: &T)
where T: serde::Serialize + for<'a> serde::Deserialize<'a> + Eq {
    let val = serde_json::to_vec(t).unwrap();
    let new_t: T = serde_json::from_slice(&val).unwrap();
    assert!(t == &new_t)
}
