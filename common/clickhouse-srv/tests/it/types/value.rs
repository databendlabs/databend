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

use std::convert;
use std::fmt;
use std::sync::Arc;

use chrono::prelude::*;
use chrono_tz::Tz;
use chrono_tz::UTC;
use common_clickhouse_srv::types::*;
use rand::distributions::Distribution;
use rand::distributions::Standard;
use rand::random;
use uuid::Uuid;

fn test_into_t<T>(v: Value, x: &T)
where
    Value: convert::Into<T>,
    T: PartialEq + fmt::Debug,
{
    let a: T = v.into();
    assert_eq!(a, *x);
}

fn test_from_rnd<T>()
where
    Value: convert::Into<T> + convert::From<T>,
    T: PartialEq + fmt::Debug + Clone,
    Standard: Distribution<T>,
{
    for _ in 0..100 {
        let value = random::<T>();
        test_into_t::<T>(Value::from(value.clone()), &value);
    }
}

fn test_from_t<T>(value: &T)
where
    Value: convert::Into<T> + convert::From<T>,
    T: PartialEq + fmt::Debug + Clone,
{
    test_into_t::<T>(Value::from(value.clone()), value);
}

macro_rules! test_type {
        ( $( $k:ident : $t:ident ),* ) => {
            $(
                #[test]
                fn $k() {
                    test_from_rnd::<$t>();
                }
            )*
        };
    }

test_type! {
    test_u8: u8,
    test_u16: u16,
    test_u32: u32,
    test_u64: u64,

    test_i8: i8,
    test_i16: i16,
    test_i32: i32,
    test_i64: i64,

    test_f32: f32,
    test_f64: f64
}

#[test]
fn test_string() {
    test_from_t(&"284222f9-aba2-4b05-bcf5-e4e727fe34d1".to_string());
}

#[test]
fn test_time() {
    test_from_t(&Tz::Africa__Addis_Ababa.ymd(2016, 10, 22).and_hms(12, 0, 0));
}

#[test]
fn test_from_u32() {
    let v = Value::UInt32(32);
    let u: u32 = u32::from(v);
    assert_eq!(u, 32);
}

#[test]
fn test_uuid() {
    let uuid = Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap();
    let mut buffer = *uuid.as_bytes();
    buffer[..8].reverse();
    buffer[8..].reverse();
    let v = Value::Uuid(buffer);
    assert_eq!(v.to_string(), "936da01f-9abd-4d9d-80c7-02af85c822a8");
}

#[test]
fn test_from_date() {
    let date_value: Date<Tz> = UTC.ymd(2016, 10, 22);
    let date_time_value: DateTime<Tz> = UTC.ymd(2014, 7, 8).and_hms(14, 0, 0);

    let d: Value = Value::from(date_value);
    let dt: Value = date_time_value.into();

    assert_eq!(
        Value::Date(u16::get_days(date_value), date_value.timezone()),
        d
    );
    assert_eq!(
        Value::DateTime(
            date_time_value.timestamp() as u32,
            date_time_value.timezone()
        ),
        dt
    );
}

#[test]
fn test_string_from() {
    let v = Value::String(Arc::new(b"df47a455-bb3c-4bd6-b2f2-a24be3db36ab".to_vec()));
    let u = String::from(v);
    assert_eq!("df47a455-bb3c-4bd6-b2f2-a24be3db36ab".to_string(), u);
}

#[test]
fn test_into_string() {
    let v = Value::String(Arc::new(b"d2384838-dfe8-43ea-b1f7-63fb27b91088".to_vec()));
    let u: String = v.into();
    assert_eq!("d2384838-dfe8-43ea-b1f7-63fb27b91088".to_string(), u);
}

#[test]
fn test_into_vec() {
    let v = Value::String(Arc::new(vec![1, 2, 3]));
    let u: Vec<u8> = v.into();
    assert_eq!(vec![1, 2, 3], u);
}

#[test]
fn test_display() {
    assert_eq!("42".to_string(), format!("{}", Value::UInt8(42)));
    assert_eq!("42".to_string(), format!("{}", Value::UInt16(42)));
    assert_eq!("42".to_string(), format!("{}", Value::UInt32(42)));
    assert_eq!("42".to_string(), format!("{}", Value::UInt64(42)));

    assert_eq!("42".to_string(), format!("{}", Value::Int8(42)));
    assert_eq!("42".to_string(), format!("{}", Value::Int16(42)));
    assert_eq!("42".to_string(), format!("{}", Value::Int32(42)));
    assert_eq!("42".to_string(), format!("{}", Value::Int64(42)));

    assert_eq!(
        "text".to_string(),
        format!("{}", Value::String(Arc::new(b"text".to_vec())))
    );

    assert_eq!(
        "\u{1}\u{2}\u{3}".to_string(),
        format!("{}", Value::String(Arc::new(vec![1, 2, 3])))
    );

    assert_eq!(
        "NULL".to_string(),
        format!("{}", Value::Nullable(Either::Left(SqlType::UInt8.into())))
    );
    assert_eq!(
        "42".to_string(),
        format!(
            "{}",
            Value::Nullable(Either::Right(Box::new(Value::UInt8(42))))
        )
    );

    assert_eq!(
        "[1, 2, 3]".to_string(),
        format!(
            "{}",
            Value::Array(
                SqlType::Int32.into(),
                Arc::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
            )
        )
    );
}

#[test]
fn test_default_fixed_str() {
    for n in 0_usize..1000_usize {
        let actual = Value::default(SqlType::FixedString(n));
        let actual_str: String = actual.into();
        assert_eq!(actual_str.len(), n);
        for ch in actual_str.as_bytes() {
            assert_eq!(*ch, 0_u8);
        }
    }
}

#[test]
fn test_size_of() {
    use std::mem;
    assert_eq!(32, mem::size_of::<[Value; 1]>());
}

#[test]
fn test_from_some() {
    assert_eq!(
        Value::from(Some(1_u32)),
        Value::Nullable(Either::Right(Value::UInt32(1).into()))
    );
    assert_eq!(
        Value::from(Some("text")),
        Value::Nullable(Either::Right(Value::String(b"text".to_vec().into()).into()))
    );
    assert_eq!(
        Value::from(Some(3.1)),
        Value::Nullable(Either::Right(Value::Float64(3.1).into()))
    );
    assert_eq!(
        Value::from(Some(UTC.ymd(2019, 1, 1).and_hms(0, 0, 0))),
        Value::Nullable(Either::Right(
            Value::DateTime(1_546_300_800, Tz::UTC).into()
        ))
    );
}
