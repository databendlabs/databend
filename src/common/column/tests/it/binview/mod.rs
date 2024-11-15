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

// mod mutable;
mod mutable_values;
// mod to_mutable;

use std::sync::Arc;

use databend_common_column::binview::BinaryViewColumn;
use databend_common_column::binview::Utf8ViewColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;

#[test]
fn basics_string_view() {
    let data = vec![
        "hello",
        "",
        // larger than 12 bytes.
        "Databend Cloud is a Cost-Effective alternative to Snowflake.",
    ];

    let array: Utf8ViewColumn = data.into_iter().collect();

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

    let array2 = Utf8ViewColumn::new_unchecked(
        array.views().clone(),
        array.data_buffers().clone(),
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
        b"hello".to_vec(),
        b"".to_vec(),
        // larger than 12 bytes.
        b"Databend Cloud is a Cost-Effective alternative to Snowflake.".to_vec(),
    ];

    let array: BinaryViewColumn = data.into_iter().collect();

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

    let array2 = BinaryViewColumn::new_unchecked(
        array.views().clone(),
        array.data_buffers().clone(),
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
    let array1 = Utf8ViewColumn::from(["hello", " ", ""]);
    let array2 = BinaryViewColumn::from([b"hello".to_vec(), b" ".to_vec(), b"".to_vec()]);
    assert_eq!(array1.to_binview(), array2);
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(b"hello").take(2);
    let a: BinaryViewColumn = iter.collect();
    assert_eq!(a.len(), 2);
}

#[test]
fn test_slice() {
    let data = vec!["hello", "world", "databend", "y", "z", "abc"];

    let array: Utf8ViewColumn = data.into_iter().collect();

    let a3 = array.sliced(2, 3);
    assert_eq!(a3.into_iter().collect::<Vec<_>>(), vec![
        "databend", "y", "z"
    ]);
}
