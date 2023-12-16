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

mod mutable;

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;

fn data() -> FixedSizeListArray {
    let values = Int32Array::from_slice([10, 20, 0, 0]);

    FixedSizeListArray::try_new(
        DataType::FixedSizeList(
            Box::new(Field::new("a", values.data_type().clone(), true)),
            2,
        ),
        values.boxed(),
        Some([true, false].into()),
    )
    .unwrap()
}

#[test]
fn basics() {
    let array = data();
    assert_eq!(array.size(), 2);
    assert_eq!(array.len(), 2);
    assert_eq!(array.validity(), Some(&Bitmap::from([true, false])));

    assert_eq!(array.value(0).as_ref(), Int32Array::from_slice([10, 20]));
    assert_eq!(array.value(1).as_ref(), Int32Array::from_slice([0, 0]));

    let array = array.sliced(1, 1);

    assert_eq!(array.value(0).as_ref(), Int32Array::from_slice([0, 0]));
}

#[test]
fn with_validity() {
    let array = data();

    let a = array.with_validity(None);
    assert!(a.validity().is_none());
}

#[test]
fn debug() {
    let array = data();

    assert_eq!(format!("{array:?}"), "FixedSizeListArray[[10, 20], None]");
}

#[test]
fn empty() {
    let array = FixedSizeListArray::new_empty(DataType::FixedSizeList(
        Box::new(Field::new("a", DataType::Int32, true)),
        2,
    ));
    assert_eq!(array.values().len(), 0);
    assert_eq!(array.validity(), None);
}

#[test]
fn null() {
    let array = FixedSizeListArray::new_null(
        DataType::FixedSizeList(Box::new(Field::new("a", DataType::Int32, true)), 2),
        2,
    );
    assert_eq!(array.values().len(), 4);
    assert_eq!(array.validity().cloned(), Some([false, false].into()));
}

#[test]
fn wrong_size() {
    let values = Int32Array::from_slice([10, 20, 0]);
    assert!(
        FixedSizeListArray::try_new(
            DataType::FixedSizeList(Box::new(Field::new("a", DataType::Int32, true)), 2),
            values.boxed(),
            None
        )
        .is_err()
    );
}

#[test]
fn wrong_len() {
    let values = Int32Array::from_slice([10, 20, 0]);
    assert!(
        FixedSizeListArray::try_new(
            DataType::FixedSizeList(Box::new(Field::new("a", DataType::Int32, true)), 2),
            values.boxed(),
            Some([true, false, false].into()), // it should be 2
        )
        .is_err()
    );
}

#[test]
fn wrong_data_type() {
    let values = Int32Array::from_slice([10, 20, 0]);
    assert!(
        FixedSizeListArray::try_new(
            DataType::Binary,
            values.boxed(),
            Some([true, false, false].into()), // it should be 2
        )
        .is_err()
    );
}
