// Copyright 2022 Datafuse Labs.
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

use std::borrow::Cow;

use common_jsonb::from_slice;

#[test]
fn test_decode_null() {
    let s = b"\x20\0\0\0\0\0\0\0";
    let value = from_slice(s).unwrap();
    assert!(value.is_null());
    assert_eq!(value.as_null(), Some(()));
}

#[test]
fn test_decode_boolean() {
    let s = b"\x20\0\0\0\x40\0\0\0";
    let value = from_slice(s).unwrap();
    assert!(value.is_boolean());
    assert_eq!(value.as_bool(), Some(true));

    let s = b"\x20\0\0\0\x30\0\0\0";
    let value = from_slice(s).unwrap();
    assert!(value.is_boolean());
    assert_eq!(value.as_bool(), Some(false));
}

#[test]
fn test_decode_string() {
    let s = b"\x20\0\0\0\x10\0\0\x03\x61\x73\x64";
    let value = from_slice(s).unwrap();
    assert!(value.is_string());
    assert_eq!(value.as_str(), Some(&Cow::from("asd")));
}

#[test]
fn test_decode_int64() {
    let s = b"\x20\0\0\0\x20\0\0\x01\x64";
    let value = from_slice(s).unwrap();
    assert!(value.is_i64());
    assert_eq!(value.as_i64(), Some(100i64));
}

#[test]
fn test_decode_float64() {
    let s = b"\x20\0\0\0\x20\0\0\x03\x02\x04\x7b";
    let value = from_slice(s).unwrap();
    assert!(value.is_f64());
    assert_eq!(value.as_f64(), Some(0.0123f64));
}

#[test]
fn test_decode_array() {
    let s = b"\x80\0\0\x02\x30\0\0\0\x40\0\0\0";
    let value = from_slice(s).unwrap();
    assert!(value.is_array());
    let array = value.as_array().unwrap();
    assert_eq!(array.len(), 2);
    let val0 = array.get(0).unwrap();
    assert!(val0.is_boolean());
    assert_eq!(val0.as_bool(), Some(false));
    let val1 = array.get(1).unwrap();
    assert!(val1.is_boolean());
    assert_eq!(val1.as_bool(), Some(true));
}

#[test]
fn test_decode_object() {
    let s = b"\x40\0\0\x01\x10\0\0\x03\x10\0\0\x03\x61\x73\x64\x61\x64\x66";
    let value = from_slice(s).unwrap();
    assert!(value.is_object());
    let obj = value.as_object().unwrap();
    assert_eq!(obj.len(), 1);

    let val = obj.get("asd").unwrap();
    assert!(val.is_string());
    assert_eq!(val.as_str(), Some(&Cow::from("adf")));
}
