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
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType;

#[test]
fn basics() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();
    let array: ListArray<i32> = array.into();

    let values = PrimitiveArray::<i32>::new(
        DataType::Int32,
        Buffer::from(vec![1, 2, 3, 4, 0, 6]),
        Some(Bitmap::from([true, true, true, true, false, true])),
    );

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let expected = ListArray::<i32>::new(
        data_type,
        vec![0, 3, 3, 6].try_into().unwrap(),
        Box::new(values),
        Some(Bitmap::from([true, false, true])),
    );
    assert_eq!(expected, array);
}

#[test]
fn with_capacity() {
    let array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::with_capacity(10);
    assert!(array.offsets().capacity() >= 10);
    assert_eq!(array.offsets().len_proxy(), 0);
    assert_eq!(array.values().values().capacity(), 0);
    assert_eq!(array.validity(), None);
}

#[test]
fn push() {
    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array
        .try_push(Some(vec![Some(1i32), Some(2), Some(3)]))
        .unwrap();
    assert_eq!(array.len(), 1);
    assert_eq!(array.values().values().as_ref(), [1, 2, 3]);
    assert_eq!(array.offsets().as_slice(), [0, 3]);
    assert_eq!(array.validity(), None);
}

#[test]
fn extend_from_self() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];
    let mut a = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    a.try_extend(data.clone()).unwrap();

    a.try_extend_from_self(&a.clone()).unwrap();
    let a: ListArray<i32> = a.into();

    let mut expected = data.clone();
    expected.extend(data);

    let mut b = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    b.try_extend(expected).unwrap();
    let b: ListArray<i32> = b.into();

    assert_eq!(a, b);
}
