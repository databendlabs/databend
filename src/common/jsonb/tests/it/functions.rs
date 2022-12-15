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
use std::cmp::Ordering;

use common_jsonb::array_length;
use common_jsonb::as_bool;
use common_jsonb::as_null;
use common_jsonb::as_number;
use common_jsonb::as_str;
use common_jsonb::build_array;
use common_jsonb::build_object;
use common_jsonb::compare;
use common_jsonb::from_slice;
use common_jsonb::get_by_name_ignore_case;
use common_jsonb::get_by_path;
use common_jsonb::is_array;
use common_jsonb::is_object;
use common_jsonb::object_keys;
use common_jsonb::parse_json_path;
use common_jsonb::parse_value;
use common_jsonb::to_string;
use common_jsonb::Error;
use common_jsonb::JsonPath;
use common_jsonb::Number;
use common_jsonb::Object;
use common_jsonb::Value;

#[test]
fn test_build_array() {
    let sources = vec![
        r#"true"#,
        r#"123.45"#,
        r#""abc""#,
        r#"[1,2,3]"#,
        r#"{"k":"v"}"#,
    ];
    let mut expect_array = Vec::with_capacity(sources.len());
    let mut offsets = Vec::with_capacity(sources.len());
    let mut buf: Vec<u8> = Vec::new();
    for s in sources {
        let value = parse_value(s.as_bytes()).unwrap();
        expect_array.push(value.clone());
        value.to_vec(&mut buf);
        offsets.push(buf.len());
    }
    let mut values = Vec::with_capacity(offsets.len());
    let mut last_offset = 0;
    for offset in offsets {
        values.push(&buf[last_offset..offset]);
        last_offset = offset;
    }

    let expect_value = Value::Array(expect_array);
    let mut expect_buf: Vec<u8> = Vec::new();
    expect_value.to_vec(&mut expect_buf);

    let mut arr_buf = Vec::new();
    build_array(values, &mut arr_buf).unwrap();
    assert_eq!(arr_buf, expect_buf);

    let value = from_slice(&arr_buf).unwrap();
    assert!(value.is_array());
    let array = value.as_array().unwrap();
    assert_eq!(array.len(), 5);
}

#[test]
fn test_build_object() {
    let sources = vec![
        r#"true"#,
        r#"123.45"#,
        r#""abc""#,
        r#"[1,2,3]"#,
        r#"{"k":"v"}"#,
    ];
    let keys = vec![
        "k1".to_string(),
        "k2".to_string(),
        "k3".to_string(),
        "k4".to_string(),
        "k5".to_string(),
    ];

    let mut buf: Vec<u8> = Vec::new();
    let mut offsets = Vec::with_capacity(sources.len());
    let mut expect_object = Object::new();
    for (key, s) in keys.iter().zip(sources.iter()) {
        let value = parse_value(s.as_bytes()).unwrap();
        expect_object.insert(key.clone(), value.clone());

        value.to_vec(&mut buf);
        offsets.push(buf.len());
    }

    let mut values = Vec::with_capacity(offsets.len());
    let mut last_offset = 0;
    for (key, offset) in keys.iter().zip(offsets.iter()) {
        values.push((key.as_str(), &buf[last_offset..*offset]));
        last_offset = *offset;
    }

    let expect_value = Value::Object(expect_object);
    let mut expect_buf: Vec<u8> = Vec::new();
    expect_value.to_vec(&mut expect_buf);

    let mut obj_buf = Vec::new();
    build_object(values, &mut obj_buf).unwrap();
    assert_eq!(obj_buf, expect_buf);

    let value = from_slice(&obj_buf).unwrap();
    assert!(value.is_object());
    let array = value.as_object().unwrap();
    assert_eq!(array.len(), 5);
}

#[test]
fn test_array_length() {
    let sources = vec![
        (r#"true"#, None),
        (r#"1234"#, None),
        (r#"[]"#, Some(0)),
        (r#"[1,2,3]"#, Some(3)),
        (r#"["a","b","c","d","e","f"]"#, Some(6)),
        (r#"{"k":"v"}"#, None),
    ];

    let mut buf: Vec<u8> = Vec::new();
    for (s, expect) in sources {
        let res = array_length(s.as_bytes());
        assert_eq!(res, expect);
        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf);
        let res = array_length(&buf);
        assert_eq!(res, expect);
        buf.clear();
    }
}

#[test]
fn test_get_by_path() {
    let sources = vec![
        (r#"1234"#, vec![JsonPath::UInt64(0)], None),
        (r#"[]"#, vec![JsonPath::UInt64(0)], None),
        (
            r#"["a","b","c"]"#,
            vec![JsonPath::UInt64(0)],
            Some(Value::String(Cow::from("a"))),
        ),
        (
            r#"{"k1":["a","b","c"], "k2":{"k3":3,"k4":4}}"#,
            vec![JsonPath::String(Cow::from("k1")), JsonPath::UInt64(0)],
            Some(Value::String(Cow::from("a"))),
        ),
        (
            r#"{"k1":["a","b","c"], "k2":{"k3":"v3","k4":"v4"}}"#,
            vec![
                JsonPath::String(Cow::from("k2")),
                JsonPath::String(Cow::from("k3")),
            ],
            Some(Value::String(Cow::from("v3"))),
        ),
    ];

    let mut buf: Vec<u8> = Vec::new();
    for (s, paths, expect) in sources {
        let res = get_by_path(s.as_bytes(), paths.clone());
        match expect.clone() {
            Some(expect) => assert_eq!(from_slice(&res.unwrap()).unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf);
        let res = get_by_path(&buf, paths);
        match expect {
            Some(expect) => assert_eq!(from_slice(&res.unwrap()).unwrap(), expect),
            None => assert_eq!(res, None),
        }
        buf.clear();
    }
}

#[test]
fn test_get_by_name_ignore_case() {
    let sources = vec![
        (r#"true"#, "a".to_string(), None),
        (r#"[1,2,3]"#, "a".to_string(), None),
        (r#"{"a":"v1","b":[1,2,3]}"#, "k".to_string(), None),
        (
            r#"{"Aa":"v1", "aA":"v2", "aa":"v3"}"#,
            "aa".to_string(),
            Some(Value::String(Cow::from("v3"))),
        ),
        (
            r#"{"Aa":"v1", "aA":"v2", "aa":"v3"}"#,
            "AA".to_string(),
            Some(Value::String(Cow::from("v1"))),
        ),
    ];

    let mut buf: Vec<u8> = Vec::new();
    for (s, name, expect) in sources {
        let res = get_by_name_ignore_case(s.as_bytes(), &name);
        match expect.clone() {
            Some(expect) => assert_eq!(from_slice(&res.unwrap()).unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf);
        let res = get_by_name_ignore_case(&buf, &name);
        match expect {
            Some(expect) => assert_eq!(from_slice(&res.unwrap()).unwrap(), expect),
            None => assert_eq!(res, None),
        }
        buf.clear();
    }
}

#[test]
fn test_object_keys() {
    let sources = vec![
        (r#"[1,2,3]"#, None),
        (
            r#"{"a":"v1","b":[1,2,3]}"#,
            Some(Value::Array(vec![
                Value::String(Cow::from("a")),
                Value::String(Cow::from("b")),
            ])),
        ),
        (
            r#"{"k1":"v1","k2":[1,2,3]}"#,
            Some(Value::Array(vec![
                Value::String(Cow::from("k1")),
                Value::String(Cow::from("k2")),
            ])),
        ),
    ];

    let mut buf: Vec<u8> = Vec::new();
    for (s, expect) in sources {
        let res = object_keys(s.as_bytes());
        match expect.clone() {
            Some(expect) => assert_eq!(from_slice(&res.unwrap()).unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf);
        let res = object_keys(&buf);
        match expect {
            Some(expect) => assert_eq!(from_slice(&res.unwrap()).unwrap(), expect),
            None => assert_eq!(res, None),
        }
        buf.clear();
    }
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
    for (l, r, expect) in sources {
        let res = compare(l.as_bytes(), r.as_bytes()).unwrap();
        assert_eq!(res, expect);

        let lvalue = parse_value(l.as_bytes()).unwrap();
        lvalue.to_vec(&mut lbuf);
        let rvalue = parse_value(r.as_bytes()).unwrap();
        rvalue.to_vec(&mut rbuf);

        let res = compare(&lbuf, &rbuf).unwrap();
        assert_eq!(res, expect);

        lbuf.clear();
        rbuf.clear();
    }
}

#[test]
fn test_parse_json_path() {
    let sources = vec![
        (r#"[1][2]"#, vec![JsonPath::UInt64(1), JsonPath::UInt64(2)]),
        (r#"["k1"]["k2"]"#, vec![
            JsonPath::String(Cow::from("k1")),
            JsonPath::String(Cow::from("k2")),
        ]),
        (r#"k1.k2:k3"#, vec![
            JsonPath::String(Cow::from("k1")),
            JsonPath::String(Cow::from("k2")),
            JsonPath::String(Cow::from("k3")),
        ]),
        ("\"k1\"", vec![JsonPath::String(Cow::from("k1"))]),
        ("\"k_1\"", vec![JsonPath::String(Cow::from("k_1"))]),
        ("\"k_1k_2\"", vec![JsonPath::String(Cow::from("k_1k_2"))]),
        ("\"k1k2\"", vec![JsonPath::String(Cow::from("k1k2"))]),
        (r#"k1["k2"][1]"#, vec![
            JsonPath::String(Cow::from("k1")),
            JsonPath::String(Cow::from("k2")),
            JsonPath::UInt64(1),
        ]),
    ];

    for (s, expect) in sources {
        let path = parse_json_path(s.as_bytes()).unwrap();
        assert_eq!(&path[..], &expect[..]);
    }

    let wrong_sources = vec![
        (r#"\"\"\\k1\"\""#, Error::InvalidToken),
        (r#"\\k1\\'"#, Error::InvalidToken),
        (r#"\"kk\"1\""#, Error::InvalidToken),
    ];
    for (s, expect) in wrong_sources {
        let path = parse_json_path(s.as_bytes());
        match path {
            Ok(_) => println!(),
            Err(_) => assert_eq!(Error::InvalidToken, expect),
        }
    }
}

#[test]
fn test_as_type() {
    let sources = vec![
        (r#"null"#, Some(()), None, None, None, false, false),
        (r#"true"#, None, Some(true), None, None, false, false),
        (r#"false"#, None, Some(false), None, None, false, false),
        (
            r#"-1234"#,
            None,
            None,
            Some(Number::Int64(-1234)),
            None,
            false,
            false,
        ),
        (
            r#"12.34"#,
            None,
            None,
            Some(Number::Float64(12.34)),
            None,
            false,
            false,
        ),
        (
            r#""abcd""#,
            None,
            None,
            None,
            Some(Cow::from("abcd")),
            false,
            false,
        ),
        (r#"[1,2,3]"#, None, None, None, None, true, false),
        (r#"{"k":"v"}"#, None, None, None, None, false, true),
    ];

    let mut buf: Vec<u8> = Vec::new();
    for (s, expect_null, expect_bool, expect_number, expect_str, expect_array, expect_object) in
        sources
    {
        let res = as_null(s.as_bytes());
        match expect_null {
            Some(_) => assert!(res.is_some()),
            None => assert_eq!(res, None),
        }
        let res = as_bool(s.as_bytes());
        match expect_bool {
            Some(expect) => assert_eq!(res.unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let res = as_number(s.as_bytes());
        match expect_number.clone() {
            Some(expect) => assert_eq!(res.unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let res = as_str(s.as_bytes());
        match expect_str.clone() {
            Some(expect) => assert_eq!(res.unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let res = is_array(s.as_bytes());
        assert_eq!(res, expect_array);
        let res = is_object(s.as_bytes());
        assert_eq!(res, expect_object);

        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf);
        let res = as_null(&buf);
        match expect_null {
            Some(_) => assert!(res.is_some()),
            None => assert_eq!(res, None),
        }
        let res = as_bool(&buf);
        match expect_bool {
            Some(expect) => assert_eq!(res.unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let res = as_number(&buf);
        match expect_number.clone() {
            Some(expect) => assert_eq!(res.unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let res = as_str(&buf);
        match expect_str.clone() {
            Some(expect) => assert_eq!(res.unwrap(), expect),
            None => assert_eq!(res, None),
        }
        let res = is_array(&buf);
        assert_eq!(res, expect_array);
        let res = is_object(&buf);
        assert_eq!(res, expect_object);

        buf.clear();
    }
}

#[test]
fn test_to_string() {
    let sources = vec![
        r#"null"#,
        r#"true"#,
        r#"false"#,
        r#"1234567"#,
        r#"-1234567"#,
        r#"123.4567"#,
        r#""abcdef""#,
        r#"[1,2,3,4]"#,
        r#"["a","b",true,false,[1,2,3],{"a":"b"}]"#,
        r#"{"k1":"v1","k2":[1,2,3],"k3":{"a":"b"}}"#,
    ];
    let mut buf: Vec<u8> = Vec::new();
    for s in sources {
        let res = to_string(s.as_bytes());
        assert_eq!(res, s.to_string());

        let value = parse_value(s.as_bytes()).unwrap();
        value.to_vec(&mut buf);
        let res = to_string(&buf);
        assert_eq!(res, s.to_string());
        buf.clear();
    }
}
