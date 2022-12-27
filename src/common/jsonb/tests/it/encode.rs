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

use common_jsonb::Number;
use common_jsonb::Object;
use common_jsonb::Value;

#[test]
fn test_encode_null() {
    assert_eq!(&Value::Null.to_vec(), b"\x20\0\0\0\0\0\0\0");
}

#[test]
fn test_encode_boolean() {
    assert_eq!(&Value::Bool(true).to_vec(), b"\x20\0\0\0\x40\0\0\0");
    assert_eq!(&Value::Bool(false).to_vec(), b"\x20\0\0\0\x30\0\0\0");
}

#[test]
fn test_encode_string() {
    assert_eq!(
        &Value::String(Cow::from("asd")).to_vec(),
        b"\x20\0\0\0\x10\0\0\x03\x61\x73\x64"
    );
    assert_eq!(
        &Value::String(Cow::from("测试")).to_vec(),
        b"\x20\0\0\0\x10\0\0\x06\xE6\xB5\x8B\xE8\xAF\x95"
    );
}

#[test]
fn test_encode_int64() {
    assert_eq!(
        &Value::Number(Number::Int64(0)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x01\x00"
    );
    assert_eq!(
        &Value::Number(Number::Int64(-100)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x02\x40\x9C"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i8::MIN as i64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x02\x40\x80"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i8::MAX as i64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x02\x40\x7F"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i16::MIN as i64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x03\x40\x80\0"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i16::MAX as i64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x03\x40\x7F\xFF"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i32::MIN as i64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x05\x40\x80\0\0\0"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i32::MAX as i64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x05\x40\x7F\xFF\xFF\xFF"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i64::MIN)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x09\x40\x80\0\0\0\0\0\0\0"
    );
    assert_eq!(
        &Value::Number(Number::Int64(i64::MAX)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x09\x40\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
    );
}

#[test]
fn test_encode_uint64() {
    assert_eq!(
        &Value::Number(Number::UInt64(0)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x01\x00"
    );
    assert_eq!(
        &Value::Number(Number::UInt64(100)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x02\x50\x64"
    );
    assert_eq!(
        &Value::Number(Number::UInt64(u8::MAX as u64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x02\x50\xFF"
    );
    assert_eq!(
        &Value::Number(Number::UInt64(u16::MAX as u64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x03\x50\xFF\xFF"
    );
    assert_eq!(
        &Value::Number(Number::UInt64(u32::MAX as u64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x05\x50\xFF\xFF\xFF\xFF"
    );
    assert_eq!(
        &Value::Number(Number::UInt64(u64::MAX)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x09\x50\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"
    );
}

#[test]
fn test_encode_float64() {
    assert_eq!(
        &Value::Number(Number::Float64(f64::INFINITY)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x01\x20"
    );
    assert_eq!(
        &Value::Number(Number::Float64(f64::NEG_INFINITY)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x01\x30"
    );
    assert_eq!(
        &Value::Number(Number::Float64(0.0123f64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x09\x60\x3F\x89\x30\xBE\x0D\xED\x28\x8D"
    );
    assert_eq!(
        &Value::Number(Number::Float64(1.2e308f64)).to_vec(),
        b"\x20\0\0\0\x20\0\0\x09\x60\x7F\xE5\x5C\x57\x6D\x81\x57\x26"
    );
}

#[test]
fn test_encode_array() {
    assert_eq!(
        &Value::Array(vec![Value::Bool(false), Value::Bool(true)]).to_vec(),
        b"\x80\0\0\x02\x30\0\0\0\x40\0\0\0"
    );
}

#[test]
fn test_encode_object() {
    let mut obj1 = Object::new();
    obj1.insert("asd".to_string(), Value::String(Cow::from("adf")));
    assert_eq!(
        &Value::Object(obj1).to_vec(),
        b"\x40\0\0\x01\x10\0\0\x03\x10\0\0\x03\x61\x73\x64\x61\x64\x66"
    );
}
