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
// limitations under the License

use std::convert;
use std::fmt;
use std::mem;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::str;
use std::sync::Arc;

use chrono::prelude::*;
use chrono_tz::Tz;
use uuid::Uuid;

use crate::types::column::datetime64::to_datetime;
use crate::types::column::Either;
use crate::types::decimal::Decimal;
use crate::types::decimal::NoBits;
use crate::types::DateConverter;
use crate::types::DateTimeType;
use crate::types::Enum16;
use crate::types::Enum8;
use crate::types::HasSqlType;
use crate::types::SqlType;

pub(crate) type AppDateTime = DateTime<Tz>;
pub(crate) type AppDate = Date<Tz>;

/// Client side representation of a value of Clickhouse column.
#[derive(Clone, Debug)]
pub enum Value {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(Arc<Vec<u8>>),
    Float32(f32),
    Float64(f64),
    Date(u16, Tz),
    DateTime(u32, Tz),
    DateTime64(i64, (u32, Tz)),
    Ipv4([u8; 4]),
    Ipv6([u8; 16]),
    Uuid([u8; 16]),
    Nullable(Either<&'static SqlType, Box<Value>>),
    Array(&'static SqlType, Arc<Vec<Value>>),
    Decimal(Decimal),
    Enum8(Vec<(String, i8)>, Enum8),
    Enum16(Vec<(String, i16)>, Enum16),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::UInt8(a), Value::UInt8(b)) => *a == *b,
            (Value::UInt16(a), Value::UInt16(b)) => *a == *b,
            (Value::UInt32(a), Value::UInt32(b)) => *a == *b,
            (Value::UInt64(a), Value::UInt64(b)) => *a == *b,
            (Value::Int8(a), Value::Int8(b)) => *a == *b,
            (Value::Int16(a), Value::Int16(b)) => *a == *b,
            (Value::Int32(a), Value::Int32(b)) => *a == *b,
            (Value::Int64(a), Value::Int64(b)) => *a == *b,
            (Value::String(a), Value::String(b)) => *a == *b,
            (Value::Float32(a), Value::Float32(b)) => *a == *b,
            (Value::Float64(a), Value::Float64(b)) => *a == *b,
            (Value::Date(a, tz_a), Value::Date(b, tz_b)) => {
                let time_a = tz_a.timestamp(i64::from(*a) * 24 * 3600, 0);
                let time_b = tz_b.timestamp(i64::from(*b) * 24 * 3600, 0);
                time_a.date() == time_b.date()
            }
            (Value::DateTime(a, tz_a), Value::DateTime(b, tz_b)) => {
                let time_a = tz_a.timestamp(i64::from(*a), 0);
                let time_b = tz_b.timestamp(i64::from(*b), 0);
                time_a == time_b
            }
            (Value::Nullable(a), Value::Nullable(b)) => *a == *b,
            (Value::Array(ta, a), Value::Array(tb, b)) => *ta == *tb && *a == *b,
            (Value::Decimal(a), Value::Decimal(b)) => *a == *b,
            (Value::Enum16(values_a, val_a), Value::Enum16(values_b, val_b)) => {
                *values_a == *values_b && *val_a == *val_b
            }
            _ => false,
        }
    }
}

impl Value {
    pub(crate) fn default(sql_type: SqlType) -> Value {
        match sql_type {
            SqlType::UInt8 => Value::UInt8(0),
            SqlType::UInt16 => Value::UInt16(0),
            SqlType::UInt32 => Value::UInt32(0),
            SqlType::UInt64 => Value::UInt64(0),
            SqlType::Int8 => Value::Int8(0),
            SqlType::Int16 => Value::Int16(0),
            SqlType::Int32 => Value::Int32(0),
            SqlType::Int64 => Value::Int64(0),
            SqlType::String => Value::String(Arc::new(Vec::default())),
            SqlType::FixedString(str_len) => Value::String(Arc::new(vec![0_u8; str_len])),
            SqlType::Float32 => Value::Float32(0.0),
            SqlType::Float64 => Value::Float64(0.0),
            SqlType::Date => 0_u16.to_date(Tz::Zulu).into(),
            SqlType::DateTime(DateTimeType::DateTime64(_, _)) => {
                Value::DateTime64(0, (1, Tz::Zulu))
            }
            SqlType::DateTime(_) => 0_u32.to_date(Tz::Zulu).into(),
            SqlType::Nullable(inner) => Value::Nullable(Either::Left(inner)),
            SqlType::Array(inner) => Value::Array(inner, Arc::new(Vec::default())),
            SqlType::Decimal(precision, scale) => Value::Decimal(Decimal {
                underlying: 0,
                precision,
                scale,
                nobits: NoBits::N64,
            }),
            SqlType::Ipv4 => Value::Ipv4([0_u8; 4]),
            SqlType::Ipv6 => Value::Ipv6([0_u8; 16]),
            SqlType::Uuid => Value::Uuid([0_u8; 16]),
            SqlType::Enum8(values) => Value::Enum8(values, Enum8(0)),
            SqlType::Enum16(values) => Value::Enum16(values, Enum16(0)),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::UInt8(ref v) => fmt::Display::fmt(v, f),
            Value::UInt16(ref v) => fmt::Display::fmt(v, f),
            Value::UInt32(ref v) => fmt::Display::fmt(v, f),
            Value::UInt64(ref v) => fmt::Display::fmt(v, f),
            Value::Int8(ref v) => fmt::Display::fmt(v, f),
            Value::Int16(ref v) => fmt::Display::fmt(v, f),
            Value::Int32(ref v) => fmt::Display::fmt(v, f),
            Value::Int64(ref v) => fmt::Display::fmt(v, f),
            Value::String(ref v) => match str::from_utf8(v) {
                Ok(s) => fmt::Display::fmt(s, f),
                Err(_) => write!(f, "{:?}", v),
            },
            Value::Float32(ref v) => fmt::Display::fmt(v, f),
            Value::Float64(ref v) => fmt::Display::fmt(v, f),
            Value::DateTime(u, tz) if f.alternate() => {
                let time = tz.timestamp(i64::from(*u), 0);
                fmt::Display::fmt(&time, f)
            }
            Value::DateTime(u, tz) => {
                let time = tz.timestamp(i64::from(*u), 0);
                write!(f, "{}", time.to_rfc2822())
            }
            Value::DateTime64(value, params) => {
                let (precision, tz) = params;
                let time = to_datetime(*value, *precision, *tz);
                write!(f, "{}", time.to_rfc2822())
            }
            Value::Date(v, tz) if f.alternate() => {
                let time = tz.timestamp(i64::from(*v) * 24 * 3600, 0);
                let date = time.date();
                fmt::Display::fmt(&date, f)
            }
            Value::Date(v, tz) => {
                let time = tz.timestamp(i64::from(*v) * 24 * 3600, 0);
                let date = time.date();
                fmt::Display::fmt(&date.format("%Y-%m-%d"), f)
            }
            Value::Nullable(v) => match v {
                Either::Left(_) => write!(f, "NULL"),
                Either::Right(data) => data.fmt(f),
            },
            Value::Array(_, vs) => {
                let cells: Vec<String> = vs.iter().map(|v| format!("{}", v)).collect();
                write!(f, "[{}]", cells.join(", "))
            }
            Value::Decimal(v) => fmt::Display::fmt(v, f),
            Value::Ipv4(v) => {
                write!(f, "{}", decode_ipv4(v))
            }
            Value::Ipv6(v) => {
                write!(f, "{}", decode_ipv6(v))
            }
            Value::Uuid(v) => {
                let mut buffer = *v;
                buffer[..8].reverse();
                buffer[8..].reverse();
                match Uuid::from_slice(&buffer) {
                    Ok(uuid) => write!(f, "{}", uuid),
                    Err(e) => write!(f, "{}", e),
                }
            }
            Value::Enum8(ref _v1, ref v2) => write!(f, "Enum8, {}", v2),
            Value::Enum16(ref _v1, ref v2) => write!(f, "Enum16, {}", v2),
        }
    }
}

impl convert::From<Value> for SqlType {
    fn from(source: Value) -> Self {
        match source {
            Value::UInt8(_) => SqlType::UInt8,
            Value::UInt16(_) => SqlType::UInt16,
            Value::UInt32(_) => SqlType::UInt32,
            Value::UInt64(_) => SqlType::UInt64,
            Value::Int8(_) => SqlType::Int8,
            Value::Int16(_) => SqlType::Int16,
            Value::Int32(_) => SqlType::Int32,
            Value::Int64(_) => SqlType::Int64,
            Value::String(_) => SqlType::String,
            Value::Float32(_) => SqlType::Float32,
            Value::Float64(_) => SqlType::Float64,
            Value::Date(_, _) => SqlType::Date,
            Value::DateTime(_, _) => SqlType::DateTime(DateTimeType::DateTime32),
            Value::Nullable(d) => match d {
                Either::Left(t) => SqlType::Nullable(t),
                Either::Right(inner) => {
                    let sql_type = SqlType::from(inner.as_ref().to_owned());
                    SqlType::Nullable(sql_type.into())
                }
            },
            Value::Array(t, _) => SqlType::Array(t),
            Value::Decimal(v) => SqlType::Decimal(v.precision, v.scale),
            Value::Ipv4(_) => SqlType::Ipv4,
            Value::Ipv6(_) => SqlType::Ipv6,
            Value::Uuid(_) => SqlType::Uuid,
            Value::Enum8(values, _) => SqlType::Enum8(values),
            Value::Enum16(values, _) => SqlType::Enum16(values),
            Value::DateTime64(_, params) => {
                let (precision, tz) = params;
                SqlType::DateTime(DateTimeType::DateTime64(precision, tz))
            }
        }
    }
}

impl<T> convert::From<Option<T>> for Value
where
    Value: convert::From<T>,
    T: HasSqlType,
{
    fn from(value: Option<T>) -> Value {
        match value {
            None => {
                let default_type: SqlType = T::get_sql_type();
                Value::Nullable(Either::Left(default_type.into()))
            }
            Some(inner) => Value::Nullable(Either::Right(Box::new(inner.into()))),
        }
    }
}

macro_rules! value_from {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<$t> for Value {
                fn from(v: $t) -> Value {
                    Value::$k(v.into())
                }
            }
        )*
    };
}

impl convert::From<AppDate> for Value {
    fn from(v: AppDate) -> Value {
        Value::Date(u16::get_days(v), v.timezone())
    }
}

impl convert::From<Enum8> for Value {
    fn from(v: Enum8) -> Value {
        Value::Enum8 { 0: vec![], 1: v }
    }
}

impl convert::From<Enum16> for Value {
    fn from(v: Enum16) -> Value {
        Value::Enum16 { 0: vec![], 1: v }
    }
}

impl convert::From<AppDateTime> for Value {
    fn from(v: AppDateTime) -> Value {
        Value::DateTime(v.timestamp() as u32, v.timezone())
    }
}

impl convert::From<String> for Value {
    fn from(v: String) -> Value {
        Value::String(Arc::new(v.into_bytes()))
    }
}

impl convert::From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Value {
        Value::String(Arc::new(v))
    }
}

impl convert::From<&[u8]> for Value {
    fn from(v: &[u8]) -> Value {
        Value::String(Arc::new(v.to_vec()))
    }
}

value_from! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64,

    Decimal: Decimal
}

impl<'a> convert::From<&'a str> for Value {
    fn from(v: &'a str) -> Self {
        let bytes: Vec<u8> = v.as_bytes().into();
        Value::String(Arc::new(bytes))
    }
}

impl convert::From<Value> for String {
    fn from(mut v: Value) -> Self {
        if let Value::String(ref mut x) = &mut v {
            let mut tmp = Arc::new(Vec::new());
            mem::swap(x, &mut tmp);
            if let Ok(result) = str::from_utf8(tmp.as_ref()) {
                return result.into();
            }
        }
        let from = SqlType::from(v);
        panic!("Can't convert Value::{} into String.", from);
    }
}

impl convert::From<Value> for Vec<u8> {
    fn from(v: Value) -> Self {
        match v {
            Value::String(bs) => bs.to_vec(),
            _ => {
                let from = SqlType::from(v);
                panic!("Can't convert Value::{} into Vec<u8>.", from)
            }
        }
    }
}

macro_rules! from_value {
    ( $( $t:ty : $k:ident ),* ) => {
        $(
            impl convert::From<Value> for $t {
                fn from(v: Value) -> $t {
                    if let Value::$k(x) = v {
                        return x;
                    }
                    let from = SqlType::from(v);
                    panic!("Can't convert Value::{} into {}", from, stringify!($t))
                }
            }
        )*
    };
}

impl convert::From<Value> for AppDate {
    fn from(v: Value) -> AppDate {
        if let Value::Date(x, tz) = v {
            let time = tz.timestamp(i64::from(x) * 24 * 3600, 0);
            return time.date();
        }
        let from = SqlType::from(v);
        panic!("Can't convert Value::{} into {}", from, "AppDate")
    }
}

impl convert::From<Value> for AppDateTime {
    fn from(v: Value) -> AppDateTime {
        match v {
            Value::DateTime(u, tz) => tz.timestamp(i64::from(u), 0),
            Value::DateTime64(u, params) => {
                let (precision, tz) = params;
                to_datetime(u, precision, tz)
            }
            _ => {
                let from = SqlType::from(v);
                panic!("Can't convert Value::{} into {}", from, "DateTime<Tz>")
            }
        }
    }
}

from_value! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,
    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,
    f32: Float32,
    f64: Float64
}

pub(crate) fn decode_ipv4(octets: &[u8; 4]) -> Ipv4Addr {
    let mut buffer = *octets;
    buffer.reverse();
    Ipv4Addr::from(buffer)
}

pub(crate) fn decode_ipv6(octets: &[u8; 16]) -> Ipv6Addr {
    Ipv6Addr::from(*octets)
}

#[cfg(test)]
mod test {
    use std::fmt;

    use chrono_tz::Tz::UTC;
    use chrono_tz::Tz::{self};
    use rand::distributions::Distribution;
    use rand::distributions::Standard;
    use rand::random;

    use super::*;

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
        test_into_t::<T>(Value::from(value.clone()), &value);
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
}
