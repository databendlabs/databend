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

use databend_common_arrow::arrow::array::MutableArray;
use databend_common_arrow::arrow::array::MutableBinaryValuesArray;
use databend_common_arrow::arrow::datatypes::DataType;

#[test]
fn capacity() {
    let mut b = MutableBinaryValuesArray::<i32>::with_capacity(100);

    assert_eq!(b.values().capacity(), 0);
    assert!(b.offsets().capacity() >= 100);
    b.shrink_to_fit();
    assert!(b.offsets().capacity() < 100);
}

#[test]
fn offsets_must_be_in_bounds() {
    let offsets = vec![0, 10].try_into().unwrap();
    let values = b"abbbbb".to_vec();
    assert!(MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values).is_err());
}

#[test]
fn data_type_must_be_consistent() {
    let offsets = vec![0, 4].try_into().unwrap();
    let values = b"abbb".to_vec();
    assert!(MutableBinaryValuesArray::<i32>::try_new(DataType::Int32, offsets, values).is_err());
}

#[test]
fn as_box() {
    let offsets = vec![0, 2].try_into().unwrap();
    let values = b"ab".to_vec();
    let mut b =
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values).unwrap();
    let _ = b.as_box();
}

#[test]
fn as_arc() {
    let offsets = vec![0, 2].try_into().unwrap();
    let values = b"ab".to_vec();
    let mut b =
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values).unwrap();
    let _ = b.as_arc();
}

#[test]
fn extend_trusted_len() {
    let offsets = vec![0, 2].try_into().unwrap();
    let values = b"ab".to_vec();
    let mut b =
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values).unwrap();
    b.extend_trusted_len(vec!["a", "b"].into_iter());

    let offsets = vec![0, 2, 3, 4].try_into().unwrap();
    let values = b"abab".to_vec();
    assert_eq!(
        b.as_box(),
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values)
            .unwrap()
            .as_box()
    )
}

#[test]
fn from_trusted_len() {
    let mut b = MutableBinaryValuesArray::<i32>::from_trusted_len_iter(vec!["a", "b"].into_iter());

    let offsets = vec![0, 1, 2].try_into().unwrap();
    let values = b"ab".to_vec();
    assert_eq!(
        b.as_box(),
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values)
            .unwrap()
            .as_box()
    )
}

#[test]
fn extend_from_iter() {
    let offsets = vec![0, 2].try_into().unwrap();
    let values = b"ab".to_vec();
    let mut b =
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values).unwrap();
    b.extend_trusted_len(vec!["a", "b"].into_iter());

    let a = b.clone();
    b.extend_trusted_len(a.iter());

    let offsets = vec![0, 2, 3, 4, 6, 7, 8].try_into().unwrap();
    let values = b"abababab".to_vec();
    assert_eq!(
        b.as_box(),
        MutableBinaryValuesArray::<i32>::try_new(DataType::Binary, offsets, values)
            .unwrap()
            .as_box()
    )
}
