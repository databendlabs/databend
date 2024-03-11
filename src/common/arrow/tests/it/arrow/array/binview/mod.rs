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
mod mutable_values;
mod to_mutable;

use std::sync::Arc;

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::BinaryViewArray;
use databend_common_arrow::arrow::array::Utf8ViewArray;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType;

#[test]
fn basics_string_view() {
    let data = vec![
        Some("hello"),
        None,
        // larger than 12 bytes.
        Some("Databend Cloud is a Cost-Effective alternative to Snowflake."),
    ];

    let array: Utf8ViewArray = data.into_iter().collect();

    assert_eq!(array.value(0), "hello");
    assert_eq!(array.value(1), "");
    assert_eq!(
        array.value(2),
        "Databend Cloud is a Cost-Effective alternative to Snowflake."
    );
    assert_eq!(
        unsafe { array.value_unchecked(2) },
        "Databend Cloud is a Cost-Effective alternative to Snowflake."
    );
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice([0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = Utf8ViewArray::new_unchecked(
        DataType::Utf8View,
        array.views().clone(),
        array.data_buffers().clone(),
        array.validity().cloned(),
        array.total_bytes_len(),
        array.total_buffer_len(),
    );

    assert_eq!(array, array2);

    let array = array.sliced(1, 2);

    assert_eq!(array.value(0), "");
    assert_eq!(
        array.value(1),
        "Databend Cloud is a Cost-Effective alternative to Snowflake."
    );
}

#[test]
fn basics_binary_view() {
    let data = vec![
        Some(b"hello".to_vec()),
        None,
        // larger than 12 bytes.
        Some(b"Databend Cloud is a Cost-Effective alternative to Snowflake.".to_vec()),
    ];

    let array: BinaryViewArray = data.into_iter().collect();

    assert_eq!(array.value(0), b"hello");
    assert_eq!(array.value(1), b"");
    assert_eq!(
        array.value(2),
        b"Databend Cloud is a Cost-Effective alternative to Snowflake."
    );
    assert_eq!(
        unsafe { array.value_unchecked(2) },
        b"Databend Cloud is a Cost-Effective alternative to Snowflake."
    );
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice([0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = BinaryViewArray::new_unchecked(
        DataType::BinaryView,
        array.views().clone(),
        array.data_buffers().clone(),
        array.validity().cloned(),
        array.total_bytes_len(),
        array.total_buffer_len(),
    );

    assert_eq!(array, array2);

    let array = array.sliced(1, 2);

    assert_eq!(array.value(0), b"");
    assert_eq!(
        array.value(1),
        b"Databend Cloud is a Cost-Effective alternative to Snowflake."
    );
}

#[test]
fn from() {
    let array = Utf8ViewArray::from([Some("hello"), Some(" "), None]);

    let a = array.validity().unwrap();
    assert_eq!(a, &Bitmap::from([true, true, false]));

    let array = BinaryViewArray::from([Some(b"hello".to_vec()), Some(b" ".to_vec()), None]);

    let a = array.validity().unwrap();
    assert_eq!(a, &Bitmap::from([true, true, false]));
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(b"hello").take(2).map(Some);
    let a: BinaryViewArray = iter.collect();
    assert_eq!(a.len(), 2);
}

#[test]
fn with_validity() {
    let array = BinaryViewArray::from([Some(b"hello".as_ref()), Some(b" ".as_ref()), None]);

    let array = array.with_validity(None);

    let a = array.validity();
    assert_eq!(a, None);
}

#[test]
#[should_panic]
fn wrong_data_type() {
    let validity = Some(Bitmap::new_zeroed(3));
    BinaryViewArray::try_new(DataType::Int8, Buffer::zeroed(3), Arc::from([]), validity).unwrap();
}

#[test]
fn debug() {
    let data = vec![Some([1_u8, 2_u8].to_vec()), Some(vec![]), None];

    let array: BinaryViewArray = data.into_iter().collect();

    assert_eq!(format!("{array:?}"), "BinaryViewArray[[1, 2], [], None]");
}

#[test]
fn rev_iter() {
    let array = BinaryViewArray::from([Some("hello".as_bytes()), Some(" ".as_bytes()), None]);

    assert_eq!(array.into_iter().rev().collect::<Vec<_>>(), vec![
        None,
        Some(" ".as_bytes()),
        Some("hello".as_bytes())
    ]);
}

#[test]
fn iter_nth() {
    let array = BinaryViewArray::from([Some("hello"), Some(" "), None]);

    assert_eq!(array.iter().nth(1), Some(Some(" ".as_bytes())));
    assert_eq!(array.iter().nth(10), None);
}
