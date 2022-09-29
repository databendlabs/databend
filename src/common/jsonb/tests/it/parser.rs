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
use common_jsonb::Number;
use common_jsonb::Object;
use common_jsonb::Value;

fn test_parse_err(errors: &[(&str, &'static str)]) {
    for &(s, err) in errors {
        let res = parse_value(s.as_bytes());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), err);
    }
}

fn test_parse_ok(tests: Vec<(&str, Value<'_>)>) {
    for (s, val) in tests {
        assert_eq!(parse_value(s.as_bytes()).unwrap(), val);
    }
}

#[test]
fn test_parse_null() {
    test_parse_err(&[
        ("n", "EOF while parsing a value, pos 1"),
        ("nul", "EOF while parsing a value, pos 3"),
        ("nulla", "trailing characters, pos 5"),
    ]);
    test_parse_ok(vec![("null", Value::Null)]);
}

#[test]
fn test_parse_boolean() {
    test_parse_err(&[
        ("t", "EOF while parsing a value, pos 1"),
        ("truz", "expected ident, pos 4"),
        ("f", "EOF while parsing a value, pos 1"),
        ("faz", "expected ident, pos 3"),
        ("truea", "trailing characters, pos 5"),
        ("falsea", "trailing characters, pos 6"),
    ]);

    test_parse_ok(vec![
        ("true", Value::Bool(true)),
        (" true ", Value::Bool(true)),
        ("false", Value::Bool(false)),
        (" false ", Value::Bool(false)),
    ]);
}

#[test]
fn test_parse_number_errors() {
    test_parse_err(&[
        ("+", "expected value, pos 1"),
        (".", "expected value, pos 1"),
        ("-", "EOF while parsing a value, pos 1"),
        ("00", "invalid number, pos 2"),
        ("0x80", "trailing characters, pos 2"),
        ("\\0", "expected value, pos 1"),
        (".0", "expected value, pos 1"),
        ("0.", "EOF while parsing a value, pos 2"),
        ("1.", "EOF while parsing a value, pos 2"),
        ("1.a", "invalid number, pos 3"),
        ("1.e1", "invalid number, pos 3"),
        ("1e", "EOF while parsing a value, pos 2"),
        ("1e+", "EOF while parsing a value, pos 3"),
        ("1a", "trailing characters, pos 2"),
    ]);
}

#[test]
fn test_parse_i64() {
    test_parse_ok(vec![
        ("-2", Value::Number(Number::Int64(-2))),
        ("-1234", Value::Number(Number::Int64(-1234))),
        (" -1234 ", Value::Number(Number::Int64(-1234))),
        (
            &i64::MIN.to_string(),
            Value::Number(Number::Int64(i64::MIN)),
        ),
        (
            &i64::MAX.to_string(),
            Value::Number(Number::UInt64(i64::MAX as u64)),
        ),
    ]);
}

#[test]
fn test_parse_u64() {
    test_parse_ok(vec![
        ("0", Value::Number(Number::UInt64(0u64))),
        ("3", Value::Number(Number::UInt64(3u64))),
        ("1234", Value::Number(Number::UInt64(1234))),
        (
            &u64::MAX.to_string(),
            Value::Number(Number::UInt64(u64::MAX)),
        ),
    ]);
}

#[test]
fn test_parse_f64() {
    test_parse_ok(vec![
        (
            "100e777777777777777777777777777",
            Value::Number(Number::Float64(f64::INFINITY)),
        ),
        (
            "-100e777777777777777777777777777",
            Value::Number(Number::Float64(f64::NEG_INFINITY)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000",
            Value::Number(Number::Float64(f64::INFINITY)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             .0e9",
            Value::Number(Number::Float64(f64::INFINITY)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             e9",
            Value::Number(Number::Float64(f64::INFINITY)),
        ),
        ("0.0", Value::Number(Number::Float64(0.0f64))),
        ("3.0", Value::Number(Number::Float64(3.0f64))),
        ("3.1", Value::Number(Number::Float64(3.1f64))),
        ("-1.2", Value::Number(Number::Float64(-1.2f64))),
        ("0.4", Value::Number(Number::Float64(0.4f64))),
        ("0.4", Value::Number(Number::Float64(0.4f64))),
        (
            "2.638344616030823e-256",
            Value::Number(Number::Float64(2.638344616030823e-256)),
        ),
        ("3.00", Value::Number(Number::Float64(3.0f64))),
        ("0.4e5", Value::Number(Number::Float64(0.4e5))),
        ("0.4e+5", Value::Number(Number::Float64(0.4e5))),
        ("0.4e15", Value::Number(Number::Float64(0.4e15))),
        ("0.4e+15", Value::Number(Number::Float64(0.4e15))),
        ("0.4e-01", Value::Number(Number::Float64(0.4e-1))),
        (" 0.4e-01 ", Value::Number(Number::Float64(0.4e-1))),
        ("0.4e-001", Value::Number(Number::Float64(0.4e-1))),
        ("0.4e-0", Value::Number(Number::Float64(0.4e0))),
        ("0.00e00", Value::Number(Number::Float64(0.0))),
        ("0.00e+00", Value::Number(Number::Float64(0.0))),
        ("0.00e-00", Value::Number(Number::Float64(0.0))),
        ("3.5E-2147483647", Value::Number(Number::Float64(0.0))),
        (
            "0.0100000000000000000001",
            Value::Number(Number::Float64(0.01)),
        ),
        (
            &format!("{}", (i64::MIN as f64) - 1.0),
            Value::Number(Number::Float64((i64::MIN as f64) - 1.0)),
        ),
        (
            &format!("{}", (u64::MAX as f64) + 1.0),
            Value::Number(Number::Float64((u64::MAX as f64) + 1.0)),
        ),
        (
            &format!("{}", f64::EPSILON),
            Value::Number(Number::Float64(f64::EPSILON)),
        ),
        (
            "0.0000000000000000000000000000000000000000000000000123e50",
            Value::Number(Number::Float64(1.23)),
        ),
        (
            "100e-777777777777777777777777777",
            Value::Number(Number::Float64(0.0)),
        ),
        (
            "1010101010101010101010101010101010101010",
            Value::Number(Number::Float64(1.010_101_010_101_01e39)),
        ),
        (
            "0.1010101010101010101010101010101010101010",
            Value::Number(Number::Float64(0.101_010_101_010_101_01)),
        ),
        (
            "0e1000000000000000000000000000000000000000000000",
            Value::Number(Number::Float64(0.0)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             00000000",
            Value::Number(Number::Float64(1e308)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             .0e8",
            Value::Number(Number::Float64(1e308)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             e8",
            Value::Number(Number::Float64(1e308)),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000000000000000000000000000000000000000000000\
             000000000000000000e-10",
            Value::Number(Number::Float64(1e308)),
        ),
    ]);
}

#[test]
fn test_parse_string() {
    test_parse_err(&[
        ("\"", "EOF while parsing a value, pos 1"),
        ("\"lol", "EOF while parsing a value, pos 4"),
        ("\"lol\"a", "trailing characters, pos 6"),
        ("\"\\uD83C\"", "unexpected end of hex escape, pos 8"),
        (
            "\"\\uD83C\\uFFFF\"",
            "invalid surrogate in hex escape 'FFFF', pos 14",
        ),
        (
            "\"\n\"",
            "control character (\\u0000-\\u001F) found while parsing a string, pos 1",
        ),
        (
            "\"\x1F\"",
            "control character (\\u0000-\\u001F) found while parsing a string, pos 1",
        ),
    ]);

    test_parse_ok(vec![
        ("\"n\"", Value::String(Cow::from("n"))),
        ("\"\\\"\"", Value::String(Cow::from("\""))),
        ("\"\\\\\"", Value::String(Cow::from("\\"))),
        ("\"/\"", Value::String(Cow::from("/"))),
        ("\"\\b\"", Value::String(Cow::from("\x08"))),
        ("\"\\f\"", Value::String(Cow::from("\x0c"))),
        ("\"\\n\"", Value::String(Cow::from("\n"))),
        ("\"\\r\"", Value::String(Cow::from("\r"))),
        ("\"\\t\"", Value::String(Cow::from("\t"))),
        ("\"\\u000b\"", Value::String(Cow::from("\x0B"))),
        ("\"\\u000B\"", Value::String(Cow::from("\x0B"))),
        ("\"\u{3A3}\"", Value::String(Cow::from("\u{3A3}"))),
    ]);

    test_parse_ok(vec![
        ("\"\"", Value::String(Cow::from(""))),
        ("\"foo\"", Value::String(Cow::from("foo"))),
        (" \"foo\" ", Value::String(Cow::from("foo"))),
        ("\"\\\"\"", Value::String(Cow::from("\""))),
        ("\"\\b\"", Value::String(Cow::from("\x08"))),
        ("\"\\n\"", Value::String(Cow::from("\n"))),
        ("\"\\r\"", Value::String(Cow::from("\r"))),
        ("\"\\t\"", Value::String(Cow::from("\t"))),
        ("\"\\u12ab\"", Value::String(Cow::from("\u{12ab}"))),
        ("\"\\uAB12\"", Value::String(Cow::from("\u{AB12}"))),
        ("\"\\uD83C\\uDF95\"", Value::String(Cow::from("\u{1F395}"))),
    ]);
}

#[test]
fn test_parse_array() {
    test_parse_err(&[
        ("[", "EOF while parsing a value, pos 1"),
        ("[ ", "EOF while parsing a value, pos 2"),
        ("[1", "EOF while parsing a value, pos 2"),
        ("[1,", "EOF while parsing a value, pos 3"),
        ("[1,]", "expected value, pos 4"),
        ("[1 2]", "expected `,` or `]`, pos 3"),
        ("[]a", "trailing characters, pos 3"),
    ]);

    test_parse_ok(vec![
        ("[]", Value::Array(vec![])),
        ("[ ]", Value::Array(vec![])),
        ("[null]", Value::Array(vec![Value::Null])),
        (" [ null ]", Value::Array(vec![Value::Null])),
        ("[true]", Value::Array(vec![Value::Bool(true)])),
        (
            "[3,1]",
            Value::Array(vec![
                Value::Number(Number::UInt64(3)),
                Value::Number(Number::UInt64(1)),
            ]),
        ),
        (
            "[[3], [1, 2]]",
            Value::Array(vec![
                Value::Array(vec![Value::Number(Number::UInt64(3))]),
                Value::Array(vec![
                    Value::Number(Number::UInt64(1)),
                    Value::Number(Number::UInt64(2)),
                ]),
            ]),
        ),
        ("[1]", Value::Array(vec![Value::Number(Number::UInt64(1))])),
        (
            "[1, 2]",
            Value::Array(vec![
                Value::Number(Number::UInt64(1)),
                Value::Number(Number::UInt64(2)),
            ]),
        ),
        (
            "[1, 2, 3]",
            Value::Array(vec![
                Value::Number(Number::UInt64(1)),
                Value::Number(Number::UInt64(2)),
                Value::Number(Number::UInt64(3)),
            ]),
        ),
        (
            "[1, [2, 3]]",
            Value::Array(vec![
                Value::Number(Number::UInt64(1)),
                Value::Array(vec![
                    Value::Number(Number::UInt64(2)),
                    Value::Number(Number::UInt64(3)),
                ]),
            ]),
        ),
    ]);
}

#[test]
fn test_parse_object() {
    test_parse_err(&[
        ("{", "EOF while parsing a value, pos 1"),
        ("{ ", "EOF while parsing a value, pos 2"),
        ("{1", "key must be a string, pos 2"),
        ("{ \"a\"", "EOF while parsing a value, pos 5"),
        ("{\"a\"", "EOF while parsing a value, pos 4"),
        ("{\"a\" ", "EOF while parsing a value, pos 5"),
        ("{\"a\" 1", "expected `:`, pos 5"),
        ("{\"a\":", "EOF while parsing a value, pos 5"),
        ("{\"a\":1", "EOF while parsing a value, pos 6"),
        ("{\"a\":1 1", "expected `,` or `}`, pos 7"),
        ("{\"a\":1,", "EOF while parsing a value, pos 7"),
        ("{}a", "trailing characters, pos 3"),
    ]);

    let mut obj1 = Object::new();
    obj1.insert("a".to_string(), Value::Number(Number::UInt64(3)));
    let mut obj2 = Object::new();
    obj2.insert("a".to_string(), Value::Number(Number::UInt64(3)));
    obj2.insert("b".to_string(), Value::Number(Number::UInt64(4)));
    let mut obj3 = Object::new();
    let mut obj3val = Object::new();
    obj3val.insert("b".to_string(), Value::Number(Number::UInt64(3)));
    obj3val.insert("c".to_string(), Value::Number(Number::UInt64(4)));
    obj3.insert("a".to_string(), Value::Object(obj3val));
    let mut obj4 = Object::new();
    obj4.insert("c".to_string(), Value::Null);

    test_parse_ok(vec![
        ("{}", Value::Object(Object::new())),
        ("{ }", Value::Object(Object::new())),
        ("{\"a\":3}", Value::Object(obj1.clone())),
        ("{ \"a\" : 3 }", Value::Object(obj1)),
        ("{\"a\":3,\"b\":4}", Value::Object(obj2.clone())),
        (" { \"a\" : 3 , \"b\" : 4 } ", Value::Object(obj2)),
        ("{\"a\": {\"b\": 3, \"c\": 4}}", Value::Object(obj3)),
        ("{\"c\":null}", Value::Object(obj4)),
    ]);
}
