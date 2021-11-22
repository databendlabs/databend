// Copyright 2020 Datafuse Labs.
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

use common_clickhouse_srv::types::column::factory::*;
use common_clickhouse_srv::types::*;

#[test]
fn test_parse_decimal() {
    assert_eq!(parse_decimal("Decimal(9, 4)"), Some((9, 4, NoBits::N32)));
    assert_eq!(parse_decimal("Decimal(10, 4)"), Some((10, 4, NoBits::N64)));
    assert_eq!(parse_decimal("Decimal(20, 4)"), None);
    assert_eq!(parse_decimal("Decimal(2000, 4)"), None);
    assert_eq!(parse_decimal("Decimal(3, 4)"), None);
    assert_eq!(parse_decimal("Decimal(20, -4)"), None);
    assert_eq!(parse_decimal("Decimal(0)"), None);
    assert_eq!(parse_decimal("Decimal(1, 2, 3)"), None);
    assert_eq!(parse_decimal("Decimal64(9)"), Some((18, 9, NoBits::N64)));
}

#[test]
fn test_parse_array_type() {
    assert_eq!(parse_array_type("Array(UInt8)"), Some("UInt8"));
}

#[test]
fn test_parse_nullable_type() {
    assert_eq!(parse_nullable_type("Nullable(Int8)"), Some("Int8"));
    assert_eq!(parse_nullable_type("Int8"), None);
    assert_eq!(parse_nullable_type("Nullable(Nullable(Int8))"), None);
}

#[test]
fn test_parse_fixed_string() {
    assert_eq!(parse_fixed_string("FixedString(8)"), Some(8_usize));
    assert_eq!(parse_fixed_string("FixedString(zz)"), None);
    assert_eq!(parse_fixed_string("Int8"), None);
}

#[test]
fn test_parse_enum8() {
    let enum8 = "Enum8 ('a' = 1, 'b' = 2)";

    let res = parse_enum8(enum8).unwrap();
    assert_eq!(res, vec![("a".to_owned(), 1), ("b".to_owned(), 2)])
}
#[test]
fn test_parse_enum16_special_chars() {
    let enum16 = "Enum16('a_' = -128, 'b&' = 0)";

    let res = parse_enum16(enum16).unwrap();
    assert_eq!(res, vec![("a_".to_owned(), -128), ("b&".to_owned(), 0)])
}

#[test]
fn test_parse_enum8_single() {
    let enum8 = "Enum8 ('a' = 1)";

    let res = parse_enum8(enum8).unwrap();
    assert_eq!(res, vec![("a".to_owned(), 1)])
}

#[test]
fn test_parse_enum8_empty_id() {
    let enum8 = "Enum8 ('' = 1, '' = 2)";

    let res = parse_enum8(enum8).unwrap();
    assert_eq!(res, vec![("".to_owned(), 1), ("".to_owned(), 2)])
}

#[test]
fn test_parse_enum8_single_empty_id() {
    let enum8 = "Enum8 ('' = 1)";

    let res = parse_enum8(enum8).unwrap();
    assert_eq!(res, vec![("".to_owned(), 1)])
}

#[test]
fn test_parse_enum8_extra_comma() {
    let enum8 = "Enum8 ('a' = 1, 'b' = 2,)";

    assert!(dbg!(parse_enum8(enum8)).is_none());
}

#[test]
fn test_parse_enum8_empty() {
    let enum8 = "Enum8 ()";

    assert!(dbg!(parse_enum8(enum8)).is_none());
}

#[test]
fn test_parse_enum8_no_value() {
    let enum8 = "Enum8 ('a' =)";

    assert!(dbg!(parse_enum8(enum8)).is_none());
}

#[test]
fn test_parse_enum8_no_ident() {
    let enum8 = "Enum8 ( = 1)";

    assert!(dbg!(parse_enum8(enum8)).is_none());
}

#[test]
fn test_parse_enum8_starting_comma() {
    let enum8 = "Enum8 ( , 'a' = 1)";

    assert!(dbg!(parse_enum8(enum8)).is_none());
}

#[test]
fn test_parse_enum16() {
    let enum16 = "Enum16 ('a' = 1, 'b' = 2)";

    let res = parse_enum16(enum16).unwrap();
    assert_eq!(res, vec![("a".to_owned(), 1), ("b".to_owned(), 2)])
}

#[test]
fn test_parse_enum16_single() {
    let enum16 = "Enum16 ('a' = 1)";

    let res = parse_enum16(enum16).unwrap();
    assert_eq!(res, vec![("a".to_owned(), 1)])
}

#[test]
fn test_parse_enum16_empty_id() {
    let enum16 = "Enum16 ('' = 1, '' = 2)";

    let res = parse_enum16(enum16).unwrap();
    assert_eq!(res, vec![("".to_owned(), 1), ("".to_owned(), 2)])
}

#[test]
fn test_parse_enum16_single_empty_id() {
    let enum16 = "Enum16 ('' = 1)";

    let res = parse_enum16(enum16).unwrap();
    assert_eq!(res, vec![("".to_owned(), 1)])
}

#[test]
fn test_parse_enum16_extra_comma() {
    let enum16 = "Enum16 ('a' = 1, 'b' = 2,)";

    assert!(dbg!(parse_enum16(enum16)).is_none());
}

#[test]
fn test_parse_enum16_empty() {
    let enum16 = "Enum16 ()";

    assert!(dbg!(parse_enum16(enum16)).is_none());
}

#[test]
fn test_parse_enum16_no_value() {
    let enum16 = "Enum16 ('a' =)";

    assert!(dbg!(parse_enum16(enum16)).is_none());
}

#[test]
fn test_parse_enum16_no_ident() {
    let enum16 = "Enum16 ( = 1)";

    assert!(dbg!(parse_enum16(enum16)).is_none());
}

#[test]
fn test_parse_enum16_starting_comma() {
    let enum16 = "Enum16 ( , 'a' = 1)";

    assert!(dbg!(parse_enum16(enum16)).is_none());
}

#[test]
fn test_parse_date_time64() {
    let source = " DateTime64 ( 3 , 'Europe/Moscow' )";
    let res = parse_date_time64(source).unwrap();
    assert_eq!(res, (3, Some("Europe/Moscow".to_string())))
}

#[test]
fn test_parse_date_time64_without_timezone() {
    let source = " DateTime64( 5 )";
    let res = parse_date_time64(source).unwrap();
    assert_eq!(res, (5, None))
}

#[test]
fn test_parse_tuple_type() {
    let source = "Tuple(Int8,String, Float64)";
    let res = parse_tuple_type(source).unwrap();
    assert_eq!(res, vec![
        "Int8".to_owned(),
        "String".to_owned(),
        "Float64".to_owned()
    ])
}
