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

use common_jsonb::parse_value;

#[test]
fn test_parse_null() {
    let s = r#"null"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_null());
    assert_eq!(value.as_null(), Some(()));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\0\0\0\0");
}

#[test]
fn test_parse_boolean() {
    let s = r#"true"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert_eq!(value.as_bool(), Some(true));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x40\0\0\0");

    let s = r#"false"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_boolean());
    assert_eq!(value.as_bool(), Some(false));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x30\0\0\0");
}

#[test]
fn test_parse_number_int64() {
    let s = r#"-1234"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_i64());
    assert_eq!(value.as_i64(), Some(-1234));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x20\0\0\x04\x03\0\xd2\x04");

    let s = r#"34567890"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_i64());
    assert_eq!(value.as_i64(), Some(34567890));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x20\0\0\x06\x02\0\xd2\x76\x0f\x02");
}

#[test]
fn test_parse_number_float64() {
    let s = r#"0.0123"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_f64());
    assert_eq!(value.as_f64(), Some(0.0123));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x20\0\0\x03\x02\x04\x7b");

    let s = r#"12.34e5"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_f64());
    assert_eq!(value.as_f64(), Some(1234000.0));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x20\0\0\x04\0\x03\xd2\x04");
}

#[test]
fn test_parse_string() {
    let s = r#""asd""#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_string());
    assert_eq!(value.as_str(), Some(&Cow::from("asd")));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x10\0\0\x03\x61\x73\x64");

    let s = r#""\\\"abc\\\"""#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_string());
    assert_eq!(value.as_str(), Some(&Cow::from("\\\"abc\\\"")));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x10\0\0\x07\x5c\x22\x61\x62\x63\x5c\x22");

    let s = r#""测试abc""#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_string());
    assert_eq!(value.as_str(), Some(&Cow::from("测试abc")));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(
        buf,
        b"\x20\0\0\0\x10\0\0\x09\xe6\xb5\x8b\xe8\xaf\x95\x61\x62\x63"
    );

    let s = r#""\u20AC""#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_string());
    assert_eq!(value.as_str(), Some(&Cow::from("€")));

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x20\0\0\0\x10\0\0\x03\xe2\x82\xac");
}

#[test]
fn test_parse_array() {
    let s = r#"[true,12345,-200,79.1234,"asd",[]]"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_array());

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x80\0\0\x06\x40\0\0\0\x20\0\0\x02\x20\0\0\x03\x20\0\0\x05\x10\0\0\x03\x50\0\0\x04\x39\x30\x03\0\xc8\x02\x04\xc2\x12\x0c\x61\x73\x64\x80\0\0\0");

    let s = r#"[1,2,3,["a","b","c"]]"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_array());

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x80\0\0\x04\x20\0\0\x01\x20\0\0\x01\x20\0\0\x01\x50\0\0\x13\x01\x02\x03\x80\0\0\x03\x10\0\0\x01\x10\0\0\x01\x10\0\0\x01\x61\x62\x63");
}

#[test]
fn test_parse_object() {
    let s = r#"{"k1":"v1","k2":"v2"}"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_object());

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(buf, b"\x40\0\0\x02\x10\0\0\x02\x10\0\0\x02\x10\0\0\x02\x10\0\0\x02\x6b\x31\x6b\x32\x76\x31\x76\x32");

    let s = r#"{"k":"v","a":"b"}"#;
    let value = parse_value(s.as_bytes()).unwrap();
    assert!(value.is_object());

    let mut buf: Vec<u8> = Vec::new();
    value.to_vec(&mut buf).unwrap();
    assert_eq!(
        buf,
        b"\x40\0\0\x02\x10\0\0\x01\x10\0\0\x01\x10\0\0\x01\x10\0\0\x01\x61\x6b\x62\x76"
    );
}

#[test]
fn test_parse_invalid() {
    let strs = vec![
        r#"nul"#,
        r#"fals"#,
        r#"123ab"#,
        r#""abc"#,
        r#"[1,2"#,
        r#"[1 2]"#,
        r#"{"k":"v""#,
        r#"{123:"v"}"#,
        r#"{"k" "v"}"#,
    ];
    for s in strs {
        assert!(parse_value(s.as_bytes()).is_err());
    }
}
