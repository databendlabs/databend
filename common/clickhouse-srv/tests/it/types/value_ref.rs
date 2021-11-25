// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use chrono_tz::Tz;
use common_clickhouse_srv::types::*;
use uuid::Uuid;

#[test]
fn test_display() {
    assert_eq!(
        "[0, 159, 146, 150]".to_string(),
        format!("{}", ValueRef::String(&[0, 159, 146, 150]))
    );

    assert_eq!("text".to_string(), format!("{}", ValueRef::String(b"text")));

    assert_eq!("42".to_string(), format!("{}", ValueRef::UInt8(42)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::UInt16(42)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::UInt32(42)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::UInt64(42)));

    assert_eq!("42".to_string(), format!("{}", ValueRef::Int8(42)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::Int16(42)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::Int32(42)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::Int64(42)));

    assert_eq!("42".to_string(), format!("{}", ValueRef::Float32(42.0)));
    assert_eq!("42".to_string(), format!("{}", ValueRef::Float64(42.0)));

    assert_eq!(
        "NULL".to_string(),
        format!(
            "{}",
            ValueRef::Nullable(Either::Left(SqlType::UInt8.into()))
        )
    );

    assert_eq!(
        "42".to_string(),
        format!(
            "{}",
            ValueRef::Nullable(Either::Right(Box::new(ValueRef::UInt8(42))))
        )
    );

    assert_eq!(
        "[1, 2, 3]".to_string(),
        format!(
            "{}",
            ValueRef::Array(
                SqlType::Int32.into(),
                Arc::new(vec![
                    ValueRef::Int32(1),
                    ValueRef::Int32(2),
                    ValueRef::Int32(3)
                ])
            )
        )
    );

    assert_eq!(
        "(1, text, 2.3)".to_string(),
        format!(
            "{}",
            ValueRef::Tuple(Arc::new(vec![
                ValueRef::Int32(1),
                ValueRef::String(b"text"),
                ValueRef::Float64(2.3)
            ]))
        )
    );

    assert_eq!(
        "1970-01-01".to_string(),
        format!("{}", ValueRef::Date(0, Tz::Zulu))
    );

    assert_eq!(
        "1970-01-01UTC".to_string(),
        format!("{:#}", ValueRef::Date(0, Tz::Zulu))
    );

    assert_eq!(
        "1970-01-01 00:00:00".to_string(),
        format!("{}", ValueRef::DateTime(0, Tz::Zulu))
    );

    assert_eq!(
        "Thu, 01 Jan 1970 00:00:00 +0000".to_string(),
        format!("{:#}", ValueRef::DateTime(0, Tz::Zulu))
    );

    assert_eq!(
        "2.00".to_string(),
        format!("{}", ValueRef::Decimal(Decimal::of(2.0_f64, 2)))
    )
}

#[test]
fn test_size_of() {
    use std::mem;
    assert_eq!(32, mem::size_of::<[ValueRef<'_>; 1]>());
}

#[test]
fn test_value_from_ref() {
    assert_eq!(Value::from(ValueRef::UInt8(42)), Value::UInt8(42));
    assert_eq!(Value::from(ValueRef::UInt16(42)), Value::UInt16(42));
    assert_eq!(Value::from(ValueRef::UInt32(42)), Value::UInt32(42));
    assert_eq!(Value::from(ValueRef::UInt64(42)), Value::UInt64(42));

    assert_eq!(Value::from(ValueRef::Int8(42)), Value::Int8(42));
    assert_eq!(Value::from(ValueRef::Int16(42)), Value::Int16(42));
    assert_eq!(Value::from(ValueRef::Int32(42)), Value::Int32(42));
    assert_eq!(Value::from(ValueRef::Int64(42)), Value::Int64(42));

    assert_eq!(Value::from(ValueRef::Float32(42.0)), Value::Float32(42.0));
    assert_eq!(Value::from(ValueRef::Float64(42.0)), Value::Float64(42.0));

    assert_eq!(
        Value::from(ValueRef::Date(42, Tz::Zulu)),
        Value::Date(42, Tz::Zulu)
    );
    assert_eq!(
        Value::from(ValueRef::DateTime(42, Tz::Zulu)),
        Value::DateTime(42, Tz::Zulu)
    );

    assert_eq!(
        Value::from(ValueRef::Decimal(Decimal::of(2.0_f64, 4))),
        Value::Decimal(Decimal::of(2.0_f64, 4))
    );

    assert_eq!(
        Value::from(ValueRef::Array(
            SqlType::Int32.into(),
            Arc::new(vec![
                ValueRef::Int32(1),
                ValueRef::Int32(2),
                ValueRef::Int32(3)
            ])
        )),
        Value::Array(
            SqlType::Int32.into(),
            Arc::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
        )
    );

    assert_eq!(
        Value::from(ValueRef::Tuple(Arc::new(vec![
            ValueRef::Int32(1),
            ValueRef::String(b"text"),
            ValueRef::Float64(2.3)
        ]))),
        Value::Tuple(Arc::new(vec![
            Value::Int32(1),
            Value::String(Arc::new(b"text".to_vec())),
            Value::Float64(2.3)
        ]))
    )
}

#[test]
fn test_uuid() {
    let uuid = Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap();
    let mut buffer = *uuid.as_bytes();
    buffer[..8].reverse();
    buffer[8..].reverse();
    let v = ValueRef::Uuid(buffer);
    assert_eq!(v.to_string(), "936da01f-9abd-4d9d-80c7-02af85c822a8");
}

#[test]
fn test_get_sql_type() {
    assert_eq!(SqlType::from(ValueRef::UInt8(42)), SqlType::UInt8);
    assert_eq!(SqlType::from(ValueRef::UInt16(42)), SqlType::UInt16);
    assert_eq!(SqlType::from(ValueRef::UInt32(42)), SqlType::UInt32);
    assert_eq!(SqlType::from(ValueRef::UInt64(42)), SqlType::UInt64);

    assert_eq!(SqlType::from(ValueRef::Int8(42)), SqlType::Int8);
    assert_eq!(SqlType::from(ValueRef::Int16(42)), SqlType::Int16);
    assert_eq!(SqlType::from(ValueRef::Int32(42)), SqlType::Int32);
    assert_eq!(SqlType::from(ValueRef::Int64(42)), SqlType::Int64);

    assert_eq!(SqlType::from(ValueRef::Float32(42.0)), SqlType::Float32);
    assert_eq!(SqlType::from(ValueRef::Float64(42.0)), SqlType::Float64);

    assert_eq!(SqlType::from(ValueRef::String(&[])), SqlType::String);

    assert_eq!(SqlType::from(ValueRef::Date(42, Tz::Zulu)), SqlType::Date);
    assert_eq!(
        SqlType::from(ValueRef::DateTime(42, Tz::Zulu)),
        SqlType::DateTime(DateTimeType::DateTime32)
    );

    assert_eq!(
        SqlType::from(ValueRef::Decimal(Decimal::of(2.0_f64, 4))),
        SqlType::Decimal(18, 4)
    );

    assert_eq!(
        SqlType::from(ValueRef::Array(
            SqlType::Int32.into(),
            Arc::new(vec![
                ValueRef::Int32(1),
                ValueRef::Int32(2),
                ValueRef::Int32(3)
            ])
        )),
        SqlType::Array(SqlType::Int32.into())
    );

    assert_eq!(
        SqlType::from(ValueRef::Nullable(Either::Left(SqlType::UInt8.into()))),
        SqlType::Nullable(SqlType::UInt8.into())
    );

    assert_eq!(
        SqlType::from(ValueRef::Nullable(Either::Right(Box::new(ValueRef::Int8(
            42
        ))))),
        SqlType::Nullable(SqlType::Int8.into())
    );

    assert_eq!(
        SqlType::from(ValueRef::Tuple(Arc::new(vec![
            ValueRef::Int32(1),
            ValueRef::String(b"text"),
            ValueRef::Float64(2.3)
        ]))),
        SqlType::Tuple(vec![
            SqlType::Int32.into(),
            SqlType::String.into(),
            SqlType::Float64.into()
        ])
    )
}
