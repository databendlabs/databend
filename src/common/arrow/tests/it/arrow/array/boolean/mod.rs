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

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::BooleanArray;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::error::Result;

mod mutable;

#[test]
fn basics() {
    let data = vec![Some(true), None, Some(false)];

    let array: BooleanArray = data.into_iter().collect();

    assert_eq!(array.data_type(), &DataType::Boolean);

    assert!(array.value(0));
    assert!(!array.value(1));
    assert!(!array.value(2));
    assert!(!unsafe { array.value_unchecked(2) });
    assert_eq!(array.values(), &Bitmap::from_u8_slice([0b00000001], 3));
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice([0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = BooleanArray::new(
        DataType::Boolean,
        array.values().clone(),
        array.validity().cloned(),
    );
    assert_eq!(array, array2);

    let array = array.sliced(1, 2);
    assert!(!array.value(0));
    assert!(!array.value(1));
}

#[test]
fn try_new_invalid() {
    assert!(BooleanArray::try_new(DataType::Int32, [true].into(), None).is_err());
    assert!(
        BooleanArray::try_new(DataType::Boolean, [true].into(), Some([false, true].into()))
            .is_err()
    );
}

#[test]
fn with_validity() {
    let bitmap = Bitmap::from([true, false, true]);
    let a = BooleanArray::new(DataType::Boolean, bitmap, None);
    let a = a.with_validity(Some(Bitmap::from([true, false, true])));
    assert!(a.validity().is_some());
}

#[test]
fn debug() {
    let array = BooleanArray::from([Some(true), None, Some(false)]);
    assert_eq!(format!("{array:?}"), "BooleanArray[true, None, false]");
}

#[test]
fn into_mut_valid() {
    let bitmap = Bitmap::from([true, false, true]);
    let a = BooleanArray::new(DataType::Boolean, bitmap, None);
    let _ = a.into_mut().right().unwrap();

    let bitmap = Bitmap::from([true, false, true]);
    let validity = Bitmap::from([true, false, true]);
    let a = BooleanArray::new(DataType::Boolean, bitmap, Some(validity));
    let _ = a.into_mut().right().unwrap();
}

#[test]
fn into_mut_invalid() {
    let bitmap = Bitmap::from([true, false, true]);
    let _other = bitmap.clone(); // values is shared
    let a = BooleanArray::new(DataType::Boolean, bitmap, None);
    let _ = a.into_mut().left().unwrap();

    let bitmap = Bitmap::from([true, false, true]);
    let validity = Bitmap::from([true, false, true]);
    let _other = validity.clone(); // validity is shared
    let a = BooleanArray::new(DataType::Boolean, bitmap, Some(validity));
    let _ = a.into_mut().left().unwrap();
}

#[test]
fn empty() {
    let array = BooleanArray::new_empty(DataType::Boolean);
    assert_eq!(array.values().len(), 0);
    assert_eq!(array.validity(), None);
}

#[test]
fn from_trusted_len_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a = BooleanArray::from_trusted_len_iter(iter.clone());
    assert_eq!(a.len(), 2);
    let a = unsafe { BooleanArray::from_trusted_len_iter_unchecked(iter) };
    assert_eq!(a.len(), 2);
}

#[test]
fn try_from_trusted_len_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some).map(Result::Ok);
    let a = BooleanArray::try_from_trusted_len_iter(iter.clone()).unwrap();
    assert_eq!(a.len(), 2);
    let a = unsafe { BooleanArray::try_from_trusted_len_iter_unchecked(iter).unwrap() };
    assert_eq!(a.len(), 2);
}

#[test]
fn from_trusted_len_values_iter() {
    let iter = std::iter::repeat(true).take(2);
    let a = BooleanArray::from_trusted_len_values_iter(iter.clone());
    assert_eq!(a.len(), 2);
    let a = unsafe { BooleanArray::from_trusted_len_values_iter_unchecked(iter) };
    assert_eq!(a.len(), 2);
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a: BooleanArray = iter.collect();
    assert_eq!(a.len(), 2);
}

#[test]
fn into_iter() {
    let data = vec![Some(true), None, Some(false)];
    let rev = data.clone().into_iter().rev();

    let array: BooleanArray = data.clone().into_iter().collect();

    assert_eq!(array.clone().into_iter().collect::<Vec<_>>(), data);

    assert!(array.into_iter().rev().eq(rev))
}
