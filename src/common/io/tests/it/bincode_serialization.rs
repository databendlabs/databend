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

use std::io::Cursor;

use databend_common_exception::Result;
use databend_common_io::prelude::*;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct TestStruct {
    a: i32,
    b: String,
}

fn create_test_value() -> TestStruct {
    TestStruct {
        a: 42,
        b: "Hello, world!".to_string(),
    }
}

#[test]
fn test_serialize_into_buf_standard() {
    let mut buffer = Cursor::new(Vec::new());
    let value = create_test_value();

    let serialize_result = bincode_serialize_into_buf(&mut buffer, &value);
    assert!(serialize_result.is_ok());
    assert!(!buffer.get_ref().is_empty());
}

#[test]
fn test_serialize_into_buf_legacy() {
    let mut buffer = Cursor::new(Vec::new());
    let value = create_test_value();

    let serialize_result =
        bincode_serialize_into_buf_with_config(&mut buffer, &value, BincodeConfig::Legacy);
    assert!(serialize_result.is_ok());
    assert!(!buffer.get_ref().is_empty());
}

#[test]
fn test_deserialize_from_slice_standard() {
    let value = create_test_value();
    let mut buffer = Cursor::new(Vec::new());
    bincode_serialize_into_buf(&mut buffer, &value).unwrap();
    let slice = buffer.get_ref().as_slice();

    let deserialized: TestStruct = bincode_deserialize_from_slice(slice).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_deserialize_from_slice_legacy() {
    let value = create_test_value();
    let mut buffer = Cursor::new(Vec::new());
    bincode_serialize_into_buf_with_config(&mut buffer, &value, BincodeConfig::Legacy).unwrap();
    let slice = buffer.get_ref().as_slice();

    let deserialized: TestStruct =
        bincode_deserialize_from_slice_with_config(slice, BincodeConfig::Legacy).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_deserialize_updates_slice_position() {
    let first_value = create_test_value();
    let second_value = TestStruct {
        a: 24,
        b: "Goodbye, world!".to_string(),
    };

    // Serialize both values into a buffer
    let mut buffer = Cursor::new(Vec::new());
    bincode_serialize_into_buf(&mut buffer, &first_value).unwrap();
    bincode_serialize_into_buf(&mut buffer, &second_value).unwrap();

    // Create a mutable slice pointing to the buffer's content
    let mut slice = buffer.get_ref().as_slice();

    // Deserialize the first value
    let deserialized_first: TestStruct = bincode_deserialize_from_stream(&mut slice).unwrap();
    assert_eq!(first_value, deserialized_first);

    // After deserializing the first value, the slice should have been updated to point to the remainder
    let deserialized_second: TestStruct = bincode_deserialize_from_stream(&mut slice).unwrap();
    assert_eq!(second_value, deserialized_second);

    // Check if the slice is at the end (no more data to deserialize)
    assert!(slice.is_empty());
}

#[test]
fn test_deserialize_from_invalid_slice() {
    let invalid_slice = &b"invalid data"[..];
    let result: Result<TestStruct> = bincode_deserialize_from_slice(invalid_slice);
    assert!(result.is_err());
}

#[test]
fn test_serialize_legacy_deserialize_standard() {
    let value = create_test_value();
    let mut buffer = Cursor::new(Vec::new());
    bincode_serialize_into_buf_with_config(&mut buffer, &value, BincodeConfig::Legacy).unwrap();
    let slice = buffer.get_ref().as_slice();

    let deserialized: TestStruct =
        bincode_deserialize_from_slice_with_config(slice, BincodeConfig::Standard).unwrap();
    assert_ne!(value, deserialized);
}

#[test]
#[ignore]
fn test_serialize_standard_deserialize_legacy() {
    let value = create_test_value();
    let mut buffer = Cursor::new(Vec::new());
    bincode_serialize_into_buf_with_config(&mut buffer, &value, BincodeConfig::Standard).unwrap();
    let slice = buffer.get_ref().as_slice();

    let result: Result<TestStruct> =
        bincode_deserialize_from_slice_with_config(slice, BincodeConfig::Legacy);
    assert!(result.is_err());
}
