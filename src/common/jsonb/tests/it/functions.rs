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

use std::cmp::Ordering;

use common_jsonb::build_array;
use common_jsonb::compare;
use common_jsonb::from_slice;
use common_jsonb::parse_value;

#[test]
fn test_build_array() {
    let sources = vec![
        r#"true"#,
        r#"123.45"#,
        r#""abc""#,
        r#"[1,2,3]"#,
        r#"{"k":"v"}"#,
    ];
    let mut values = Vec::with_capacity(sources.len());
    let mut buf: Vec<u8> = Vec::new();
    for s in sources {
        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf).unwrap();
        let val_buf = buf.clone();
        values.push(val_buf);
        buf.clear();
    }
    let mut arr_buf = Vec::new();
    build_array(values, &mut arr_buf).unwrap();
    assert_eq!(arr_buf, b"\x80\0\0\x05\x40\0\0\x00\x20\0\0\x04\x10\0\0\x03\x50\0\0\x13\x50\0\0\x0e\x02\x02\x39\x30\x61\x62\x63\x80\0\0\x03\x20\0\0\x01\x20\0\0\x01\x20\0\0\x01\x01\x02\x03\x40\0\0\x01\x10\0\0\x01\x10\0\0\x01\x6b\x76");

    let value = from_slice(&arr_buf).unwrap();
    assert!(value.is_array());
    let array = value.as_array().unwrap();
    assert_eq!(array.len(), 5);
}

#[test]
fn test_compare() {
    let sources = vec![
        (r#"null"#, r#"null"#, Ordering::Equal),
        (r#"null"#, r#"[1,2,3]"#, Ordering::Greater),
        (r#"null"#, r#"{"k":"v"}"#, Ordering::Greater),
        (r#"null"#, r#"123.45"#, Ordering::Greater),
        (r#"null"#, r#""abcd""#, Ordering::Greater),
        (r#"null"#, r#"true"#, Ordering::Greater),
        (r#"null"#, r#"false"#, Ordering::Greater),
        (r#""abcd""#, r#"null"#, Ordering::Less),
        (r#""abcd""#, r#""def""#, Ordering::Less),
        (r#""abcd""#, r#"123.45"#, Ordering::Greater),
        (r#""abcd""#, r#"true"#, Ordering::Greater),
        (r#""abcd""#, r#"false"#, Ordering::Greater),
        (r#"123"#, r#"12.3"#, Ordering::Greater),
        (r#"123"#, r#"123"#, Ordering::Equal),
        (r#"123"#, r#"456.7"#, Ordering::Less),
        (r#"123"#, r#"true"#, Ordering::Greater),
        (r#"123"#, r#"false"#, Ordering::Greater),
        (r#"true"#, r#"true"#, Ordering::Equal),
        (r#"true"#, r#"false"#, Ordering::Greater),
        (r#"false"#, r#"true"#, Ordering::Less),
        (r#"false"#, r#"false"#, Ordering::Equal),
        (r#"[1,2,3]"#, r#"null"#, Ordering::Less),
        (r#"[1,2,3]"#, r#"[1,2,3]"#, Ordering::Equal),
        (r#"[1,2,3]"#, r#"[1,2]"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"[]"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"[3]"#, Ordering::Less),
        (r#"[1,2,3]"#, r#"["a"]"#, Ordering::Less),
        (r#"[1,2,3]"#, r#"[true]"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"[1,2,3,4]"#, Ordering::Less),
        (r#"[1,2,3]"#, r#"{"k":"v"}"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#""abcd""#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"1234"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"12.34"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"true"#, Ordering::Greater),
        (r#"[1,2,3]"#, r#"false"#, Ordering::Greater),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"null"#, Ordering::Less),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"[1,2,3]"#, Ordering::Less),
        (
            r#"{"k1":"v1","k2":"v2"}"#,
            r#"{"k1":"v1","k2":"v2"}"#,
            Ordering::Equal,
        ),
        (
            r#"{"k1":"v1","k2":"v2"}"#,
            r#"{"k":"v1","k2":"v2"}"#,
            Ordering::Less,
        ),
        (
            r#"{"k1":"v1","k2":"v2"}"#,
            r#"{"k1":"a1","k2":"v2"}"#,
            Ordering::Greater,
        ),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"{"a":1}"#, Ordering::Less),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"{}"#, Ordering::Greater),
        (r#"{"k1":"v1","k2":"v2"}"#, r#""ab""#, Ordering::Greater),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"123"#, Ordering::Greater),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"12.34"#, Ordering::Greater),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"true"#, Ordering::Greater),
        (r#"{"k1":"v1","k2":"v2"}"#, r#"false"#, Ordering::Greater),
    ];

    let mut lbuf: Vec<u8> = Vec::new();
    let mut rbuf: Vec<u8> = Vec::new();
    for (l, r, expected) in sources {
        let lvalue = parse_value(l.as_bytes()).unwrap();
        lvalue.to_vec(&mut lbuf).unwrap();
        let rvalue = parse_value(r.as_bytes()).unwrap();
        rvalue.to_vec(&mut rbuf).unwrap();
        let res = compare(&lbuf, &rbuf).unwrap();
        assert_eq!(res, expected);

        lbuf.clear();
        rbuf.clear();
    }
}
