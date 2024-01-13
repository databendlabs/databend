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

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::datatypes::DataType;

#[test]
fn basic() {
    let a = MutableFixedSizeBinaryArray::try_new(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        None,
    )
    .unwrap();
    assert_eq!(a.len(), 2);
    assert_eq!(a.data_type(), &DataType::FixedSizeBinary(2));
    assert_eq!(a.values(), &Vec::from([1, 2, 3, 4]));
    assert_eq!(a.validity(), None);
    assert_eq!(a.value(1), &[3, 4]);
    assert_eq!(unsafe { a.value_unchecked(1) }, &[3, 4]);
}

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let a = MutableFixedSizeBinaryArray::try_new(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        None,
    )
    .unwrap();
    assert_eq!(a, a);
    let b =
        MutableFixedSizeBinaryArray::try_new(DataType::FixedSizeBinary(2), Vec::from([1, 2]), None)
            .unwrap();
    assert_eq!(b, b);
    assert!(a != b);
    let a = MutableFixedSizeBinaryArray::try_new(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        Some(MutableBitmap::from([true, false])),
    )
    .unwrap();
    let b = MutableFixedSizeBinaryArray::try_new(
        DataType::FixedSizeBinary(2),
        Vec::from([1, 2, 3, 4]),
        Some(MutableBitmap::from([false, true])),
    )
    .unwrap();
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
}

#[test]
fn try_from_iter() {
    let array = MutableFixedSizeBinaryArray::try_from_iter(
        vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")],
        2,
    )
    .unwrap();
    assert_eq!(array.len(), 4);
}

#[test]
fn push_null() {
    let mut array = MutableFixedSizeBinaryArray::new(2);
    array.push::<&[u8]>(None);

    let array: FixedSizeBinaryArray = array.into();
    assert_eq!(array.validity(), Some(&Bitmap::from([false])));
}

#[test]
fn pop() {
    let mut a = MutableFixedSizeBinaryArray::new(2);
    a.push(Some(b"aa"));
    a.push::<&[u8]>(None);
    a.push(Some(b"bb"));
    a.push::<&[u8]>(None);

    assert_eq!(a.pop(), None);
    assert_eq!(a.len(), 3);
    assert_eq!(a.pop(), Some(b"bb".to_vec()));
    assert_eq!(a.len(), 2);
    assert_eq!(a.pop(), None);
    assert_eq!(a.len(), 1);
    assert_eq!(a.pop(), Some(b"aa".to_vec()));
    assert!(a.is_empty());
    assert_eq!(a.pop(), None);
    assert!(a.is_empty());
}

#[test]
fn pop_all_some() {
    let mut a = MutableFixedSizeBinaryArray::new(2);
    a.push(Some(b"aa"));
    a.push(Some(b"bb"));
    a.push(Some(b"cc"));
    a.push(Some(b"dd"));

    for _ in 0..4 {
        a.push(Some(b"11"));
    }

    a.push(Some(b"22"));

    assert_eq!(a.pop(), Some(b"22".to_vec()));
    assert_eq!(a.pop(), Some(b"11".to_vec()));
    assert_eq!(a.pop(), Some(b"11".to_vec()));
    assert_eq!(a.pop(), Some(b"11".to_vec()));
    assert_eq!(a.len(), 5);

    assert_eq!(
        a,
        MutableFixedSizeBinaryArray::try_from_iter(
            vec![
                Some(b"aa"),
                Some(b"bb"),
                Some(b"cc"),
                Some(b"dd"),
                Some(b"11"),
            ],
            2,
        )
        .unwrap()
    );
}

#[test]
fn as_arc() {
    let mut array = MutableFixedSizeBinaryArray::try_from_iter(
        vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")],
        2,
    )
    .unwrap();

    let array = array.as_arc();
    assert_eq!(array.len(), 4);
}

#[test]
fn as_box() {
    let mut array = MutableFixedSizeBinaryArray::try_from_iter(
        vec![Some(b"ab"), Some(b"bc"), None, Some(b"fh")],
        2,
    )
    .unwrap();

    let array = array.as_box();
    assert_eq!(array.len(), 4);
}

#[test]
fn shrink_to_fit_and_capacity() {
    let mut array = MutableFixedSizeBinaryArray::with_capacity(2, 100);
    array.push(Some([1, 2]));
    array.shrink_to_fit();
    assert_eq!(array.capacity(), 1);
}

#[test]
fn extend_from_self() {
    let mut a = MutableFixedSizeBinaryArray::from([Some([1u8, 2u8]), None]);

    a.try_extend_from_self(&a.clone()).unwrap();

    assert_eq!(
        a,
        MutableFixedSizeBinaryArray::from([Some([1u8, 2u8]), None, Some([1u8, 2u8]), None])
    );
}
