// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Literal values in Iceberg

use std::any::Any;
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use uuid::Uuid;

use super::Map;
use super::primitive::PrimitiveLiteral;
use super::struct_value::Struct;
use super::temporal::{date, time, timestamp, timestamptz};
use crate::error::Result;
use crate::spec::datatypes::{PrimitiveType, Type};
use crate::{Error, ErrorKind};

/// Values present in iceberg type
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Literal {
    /// A primitive value
    Primitive(PrimitiveLiteral),
    /// A struct is a tuple of typed values. Each field in the tuple is named and has an integer id that is unique in the table schema.
    /// Each field can be either optional or required, meaning that values can (or cannot) be null. Fields may be any type.
    /// Fields may have an optional comment or doc string. Fields can have default values.
    Struct(Struct),
    /// A list is a collection of values with some element type.
    /// The element field has an integer id that is unique in the table schema.
    /// Elements can be either optional or required. Element types may be any type.
    List(Vec<Option<Literal>>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    /// Both the key field and value field each have an integer id that is unique in the table schema.
    /// Map keys are required and map values can be either optional or required. Both map keys and map values may be any type, including nested types.
    Map(Map),
}

impl Literal {
    /// Creates a boolean value.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::bool(true);
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Boolean(true)), t);
    /// ```
    pub fn bool<T: Into<bool>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Boolean(t.into()))
    }

    /// Creates a boolean value from string.
    /// See [Parse bool from str](https://doc.rust-lang.org/stable/std/primitive.bool.html#impl-FromStr-for-bool) for reference.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::bool_from_str("false").unwrap();
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Boolean(false)), t);
    /// ```
    pub fn bool_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let v = s.as_ref().parse::<bool>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse string to bool.").with_source(e)
        })?;
        Ok(Self::Primitive(PrimitiveLiteral::Boolean(v)))
    }

    /// Creates an 32bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::int(23i8);
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Int(23)), t);
    /// ```
    pub fn int<T: Into<i32>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Int(t.into()))
    }

    /// Creates an 64bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t = Literal::long(24i8);
    ///
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Long(24)), t);
    /// ```
    pub fn long<T: Into<i64>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(t.into()))
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Literal::float(32.1f32);
    ///
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(32.1))),
    ///     t
    /// );
    /// ```
    pub fn float<T: Into<f32>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Float(OrderedFloat(t.into())))
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Literal::double(32.1f64);
    ///
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(32.1))),
    ///     t
    /// );
    /// ```
    pub fn double<T: Into<f64>>(t: T) -> Self {
        Self::Primitive(PrimitiveLiteral::Double(OrderedFloat(t.into())))
    }

    /// Creates date literal from number of days from unix epoch directly.
    pub fn date(days: i32) -> Self {
        Self::Primitive(PrimitiveLiteral::Int(days))
    }

    /// Creates a date in `%Y-%m-%d` format, assume in utc timezone.
    ///
    /// See [`NaiveDate::from_str`].
    ///
    /// Example
    /// ```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::date_from_str("1970-01-03").unwrap();
    ///
    /// assert_eq!(Literal::date(2), t);
    /// ```
    pub fn date_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let t = s.as_ref().parse::<NaiveDate>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse date from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;

        Ok(Self::date(date::date_from_naive_date(t)))
    }

    /// Create a date from calendar date (year, month and day).
    ///
    /// See [`NaiveDate::from_ymd_opt`].
    ///
    /// Example:
    ///
    ///```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::date_from_ymd(1970, 1, 5).unwrap();
    ///
    /// assert_eq!(Literal::date(4), t);
    /// ```
    pub fn date_from_ymd(year: i32, month: u32, day: u32) -> Result<Self> {
        let t = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't create date from year: {year}, month: {month}, day: {day}"),
            )
        })?;

        Ok(Self::date(date::date_from_naive_date(t)))
    }

    /// Creates time in microseconds directly
    pub fn time(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates time literal from [`chrono::NaiveTime`].
    fn time_from_naive_time(t: NaiveTime) -> Self {
        let duration = t - date::unix_epoch().time();
        // It's safe to unwrap here since less than 24 hours will never overflow.
        let micro_secs = duration.num_microseconds().unwrap();

        Literal::time(micro_secs)
    }

    /// Creates time in microseconds in `%H:%M:%S:.f` format.
    ///
    /// See [`NaiveTime::from_str`] for details.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::time_from_str("01:02:01.888999777").unwrap();
    ///
    /// let micro_secs = {
    ///     1 * 3600 * 1_000_000 + // 1 hour
    ///     2 * 60 * 1_000_000 +   // 2 minutes
    ///     1 * 1_000_000 + // 1 second
    ///     888999 // microseconds
    /// };
    /// assert_eq!(Literal::time(micro_secs), t);
    /// ```
    pub fn time_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let t = s.as_ref().parse::<NaiveTime>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse time from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;

        Ok(Self::time_from_naive_time(t))
    }

    /// Creates time literal from hour, minute, second, and microseconds.
    ///
    /// See [`NaiveTime::from_hms_micro_opt`].
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::Literal;
    /// let t = Literal::time_from_hms_micro(22, 15, 33, 111).unwrap();
    ///
    /// assert_eq!(Literal::time_from_str("22:15:33.000111").unwrap(), t);
    /// ```
    pub fn time_from_hms_micro(hour: u32, min: u32, sec: u32, micro: u32) -> Result<Self> {
        let t = NaiveTime::from_hms_micro_opt(hour, min, sec, micro)
            .ok_or_else(|| Error::new(
                ErrorKind::DataInvalid,
                format!("Can't create time from hour: {hour}, min: {min}, second: {sec}, microsecond: {micro}"),
            ))?;
        Ok(Self::time_from_naive_time(t))
    }

    /// Creates a timestamp from unix epoch in microseconds.
    pub fn timestamp(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp with timezone from unix epoch in microseconds.
    pub fn timestamptz(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp from unix epoch in nanoseconds.
    pub(crate) fn timestamp_nano(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp with timezone from unix epoch in nanoseconds.
    pub(crate) fn timestamptz_nano(value: i64) -> Self {
        Self::Primitive(PrimitiveLiteral::Long(value))
    }

    /// Creates a timestamp from [`DateTime`].
    pub fn timestamp_from_datetime<T: TimeZone>(dt: DateTime<T>) -> Self {
        Self::timestamp(dt.with_timezone(&Utc).timestamp_micros())
    }

    /// Creates a timestamp with timezone from [`DateTime`].
    pub fn timestamptz_from_datetime<T: TimeZone>(dt: DateTime<T>) -> Self {
        Self::timestamptz(dt.with_timezone(&Utc).timestamp_micros())
    }

    /// Parse a timestamp in RFC3339 format.
    ///
    /// See [`DateTime<Utc>::from_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
    /// use iceberg::spec::Literal;
    /// let t = Literal::timestamp_from_str("2012-12-12 12:12:12.8899-04:00").unwrap();
    ///
    /// let t2 = {
    ///     let date = NaiveDate::from_ymd_opt(2012, 12, 12).unwrap();
    ///     let time = NaiveTime::from_hms_micro_opt(12, 12, 12, 889900).unwrap();
    ///     let dt = NaiveDateTime::new(date, time);
    ///     Literal::timestamp_from_datetime(DateTime::<FixedOffset>::from_local(
    ///         dt,
    ///         FixedOffset::west_opt(4 * 3600).unwrap(),
    ///     ))
    /// };
    ///
    /// assert_eq!(t, t2);
    /// ```
    pub fn timestamp_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = DateTime::<Utc>::from_str(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse datetime.").with_source(e)
        })?;

        Ok(Self::timestamp_from_datetime(dt))
    }

    /// Similar to [`Literal::timestamp_from_str`], but return timestamp with timezone literal.
    pub fn timestamptz_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = DateTime::<Utc>::from_str(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse datetime.").with_source(e)
        })?;

        Ok(Self::timestamptz_from_datetime(dt))
    }

    /// Creates a string literal.
    pub fn string<S: ToString>(s: S) -> Self {
        Self::Primitive(PrimitiveLiteral::String(s.to_string()))
    }

    /// Creates uuid literal.
    pub fn uuid(uuid: Uuid) -> Self {
        Self::Primitive(PrimitiveLiteral::UInt128(uuid.as_u128()))
    }

    /// Creates uuid from str. See [`Uuid::parse_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Literal;
    /// use uuid::Uuid;
    /// let t1 = Literal::uuid_from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();
    /// let t2 = Literal::uuid(Uuid::from_u128_le(0xd8d7d6d5d4d3d2d1c2c1b2b1a4a3a2a1));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn uuid_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let uuid = Uuid::parse_str(s.as_ref()).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Can't parse uuid from string: {}", s.as_ref()),
            )
            .with_source(e)
        })?;
        Ok(Self::uuid(uuid))
    }

    /// Creates a fixed literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t1 = Literal::fixed(vec![1u8, 2u8]);
    /// let t2 = Literal::Primitive(PrimitiveLiteral::Binary(vec![1u8, 2u8]));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn fixed<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Literal::Primitive(PrimitiveLiteral::Binary(input.into_iter().collect()))
    }

    /// Creates a binary literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Literal, PrimitiveLiteral};
    /// let t1 = Literal::binary(vec![1u8, 2u8]);
    /// let t2 = Literal::Primitive(PrimitiveLiteral::Binary(vec![1u8, 2u8]));
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn binary<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Literal::Primitive(PrimitiveLiteral::Binary(input.into_iter().collect()))
    }

    /// Creates a decimal literal.
    pub fn decimal(decimal: i128) -> Self {
        Self::Primitive(PrimitiveLiteral::Int128(decimal))
    }

    /// Creates decimal literal from string. See [`Decimal::from_str_exact`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Literal;
    /// use rust_decimal::Decimal;
    /// let t1 = Literal::decimal(12345);
    /// let t2 = Literal::decimal_from_str("123.45").unwrap();
    ///
    /// assert_eq!(t1, t2);
    /// ```
    pub fn decimal_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let decimal = Decimal::from_str_exact(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse decimal.").with_source(e)
        })?;
        Ok(Self::decimal(decimal.mantissa()))
    }

    /// Attempts to convert the Literal to a PrimitiveLiteral
    pub fn as_primitive_literal(&self) -> Option<PrimitiveLiteral> {
        match self {
            Literal::Primitive(primitive) => Some(primitive.clone()),
            _ => None,
        }
    }

    /// Create iceberg value from a json value
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#json-single-value-serialization) for reference.
    pub fn try_from_json(value: JsonValue, data_type: &Type) -> Result<Option<Self>> {
        match data_type {
            Type::Primitive(primitive) => match (primitive, value) {
                (PrimitiveType::Boolean, JsonValue::Bool(bool)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Boolean(bool))))
                }
                (PrimitiveType::Int, JsonValue::Number(number)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        number
                            .as_i64()
                            .ok_or(Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Failed to convert json number to int",
                            ))?
                            .try_into()?,
                    ))))
                }
                (PrimitiveType::Long, JsonValue::Number(number)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Long(number.as_i64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to long",
                    ))?),
                ))),
                (PrimitiveType::Float, JsonValue::Number(number)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Float(OrderedFloat(number.as_f64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to float",
                    ))? as f32)),
                ))),
                (PrimitiveType::Double, JsonValue::Number(number)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Double(OrderedFloat(number.as_f64().ok_or(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "Failed to convert json number to double",
                    ))?)),
                ))),
                (PrimitiveType::Date, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        date::date_to_days(&NaiveDate::parse_from_str(&s, "%Y-%m-%d")?),
                    ))))
                }
                (PrimitiveType::Date, JsonValue::Number(number)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int(
                        number
                            .as_i64()
                            .ok_or(Error::new(
                                crate::ErrorKind::DataInvalid,
                                "Failed to convert json number to date (days since epoch)",
                            ))?
                            .try_into()?,
                    ))))
                }
                (PrimitiveType::Time, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Long(
                        time::time_to_microseconds(&NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")?),
                    ))))
                }
                (PrimitiveType::Timestamp, JsonValue::String(s)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::Long(timestamp::datetime_to_microseconds(
                        &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?,
                    )),
                ))),
                (PrimitiveType::Timestamptz, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Long(
                        timestamptz::datetimetz_to_microseconds(&Utc.from_utc_datetime(
                            &NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f+00:00")?,
                        )),
                    ))))
                }
                (PrimitiveType::String, JsonValue::String(s)) => {
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::String(s))))
                }
                (PrimitiveType::Uuid, JsonValue::String(s)) => Ok(Some(Literal::Primitive(
                    PrimitiveLiteral::UInt128(Uuid::parse_str(&s)?.as_u128()),
                ))),
                (PrimitiveType::Fixed(_), JsonValue::String(_)) => todo!(),
                (PrimitiveType::Binary, JsonValue::String(_)) => todo!(),
                (
                    PrimitiveType::Decimal {
                        precision: _,
                        scale,
                    },
                    JsonValue::String(s),
                ) => {
                    let mut decimal = Decimal::from_str_exact(&s)?;
                    decimal.rescale(*scale);
                    Ok(Some(Literal::Primitive(PrimitiveLiteral::Int128(
                        decimal.mantissa(),
                    ))))
                }
                (_, JsonValue::Null) => Ok(None),
                (i, j) => Err(Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("The json value {j} doesn't fit to the iceberg type {i}."),
                )),
            },
            Type::Struct(schema) => {
                if let JsonValue::Object(mut object) = value {
                    Ok(Some(Literal::Struct(Struct::from_iter(
                        schema.fields().iter().map(|field| {
                            object.remove(&field.id.to_string()).and_then(|value| {
                                Literal::try_from_json(value, &field.field_type)
                                    .and_then(|value| {
                                        value.ok_or(Error::new(
                                            ErrorKind::DataInvalid,
                                            "Key of map cannot be null",
                                        ))
                                    })
                                    .ok()
                            })
                        }),
                    ))))
                } else {
                    Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "The json value for a struct type must be an object.",
                    ))
                }
            }
            Type::List(list) => {
                if let JsonValue::Array(array) = value {
                    Ok(Some(Literal::List(
                        array
                            .into_iter()
                            .map(|value| {
                                Literal::try_from_json(value, &list.element_field.field_type)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )))
                } else {
                    Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "The json value for a list type must be an array.",
                    ))
                }
            }
            Type::Map(map) => {
                if let JsonValue::Object(mut object) = value {
                    if let (Some(JsonValue::Array(keys)), Some(JsonValue::Array(values))) =
                        (object.remove("keys"), object.remove("values"))
                    {
                        Ok(Some(Literal::Map(Map::from_iter(
                            keys.into_iter()
                                .zip(values.into_iter())
                                .map(|(key, value)| {
                                    Ok((
                                        Literal::try_from_json(key, &map.key_field.field_type)
                                            .and_then(|value| {
                                                value.ok_or(Error::new(
                                                    ErrorKind::DataInvalid,
                                                    "Key of map cannot be null",
                                                ))
                                            })?,
                                        Literal::try_from_json(value, &map.value_field.field_type)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>>>()?,
                        ))))
                    } else {
                        Err(Error::new(
                            crate::ErrorKind::DataInvalid,
                            "The json value for a list type must be an array.",
                        ))
                    }
                } else {
                    Err(Error::new(
                        crate::ErrorKind::DataInvalid,
                        "The json value for a list type must be an array.",
                    ))
                }
            }
        }
    }

    /// Converting iceberg value to json value.
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#json-single-value-serialization) for reference.
    pub fn try_into_json(self, r#type: &Type) -> Result<JsonValue> {
        match (self, r#type) {
            (Literal::Primitive(prim), Type::Primitive(prim_type)) => match (prim_type, prim) {
                (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(val)) => {
                    Ok(JsonValue::Bool(val))
                }
                (PrimitiveType::Int, PrimitiveLiteral::Int(val)) => {
                    Ok(JsonValue::Number((val).into()))
                }
                (PrimitiveType::Long, PrimitiveLiteral::Long(val)) => {
                    Ok(JsonValue::Number((val).into()))
                }
                (PrimitiveType::Float, PrimitiveLiteral::Float(val)) => {
                    match Number::from_f64(val.0 as f64) {
                        Some(number) => Ok(JsonValue::Number(number)),
                        None => Ok(JsonValue::Null),
                    }
                }
                (PrimitiveType::Double, PrimitiveLiteral::Double(val)) => {
                    match Number::from_f64(val.0) {
                        Some(number) => Ok(JsonValue::Number(number)),
                        None => Ok(JsonValue::Null),
                    }
                }
                (PrimitiveType::Date, PrimitiveLiteral::Int(val)) => {
                    Ok(JsonValue::String(date::days_to_date(val).to_string()))
                }
                (PrimitiveType::Time, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    time::microseconds_to_time(val).to_string(),
                )),
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    timestamp::microseconds_to_datetime(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f")
                        .to_string(),
                )),
                (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    timestamptz::microseconds_to_datetimetz(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                        .to_string(),
                )),
                (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(val)) => Ok(JsonValue::String(
                    timestamp::nanoseconds_to_datetime(val)
                        .format("%Y-%m-%dT%H:%M:%S%.f")
                        .to_string(),
                )),
                (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(val)) => {
                    Ok(JsonValue::String(
                        timestamptz::nanoseconds_to_datetimetz(val)
                            .format("%Y-%m-%dT%H:%M:%S%.f+00:00")
                            .to_string(),
                    ))
                }
                (PrimitiveType::String, PrimitiveLiteral::String(val)) => {
                    Ok(JsonValue::String(val.clone()))
                }
                (_, PrimitiveLiteral::UInt128(val)) => {
                    Ok(JsonValue::String(Uuid::from_u128(val).to_string()))
                }
                (_, PrimitiveLiteral::Binary(val)) => Ok(JsonValue::String(val.iter().fold(
                    String::new(),
                    |mut acc, x| {
                        acc.push_str(&format!("{x:x}"));
                        acc
                    },
                ))),
                (_, PrimitiveLiteral::Int128(val)) => match r#type {
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: _precision,
                        scale,
                    }) => {
                        let decimal = Decimal::try_from_i128_with_scale(val, *scale)?;
                        Ok(JsonValue::String(decimal.to_string()))
                    }
                    _ => Err(Error::new(
                        ErrorKind::DataInvalid,
                        "The iceberg type for decimal literal must be decimal.",
                    ))?,
                },
                _ => Err(Error::new(
                    ErrorKind::DataInvalid,
                    "The iceberg value doesn't fit to the iceberg type.",
                )),
            },
            (Literal::Struct(s), Type::Struct(struct_type)) => {
                let mut id_and_value = Vec::with_capacity(struct_type.fields().len());
                for (value, field) in s.into_iter().zip(struct_type.fields()) {
                    let json = match value {
                        Some(val) => val.try_into_json(&field.field_type)?,
                        None => JsonValue::Null,
                    };
                    id_and_value.push((field.id.to_string(), json));
                }
                Ok(JsonValue::Object(JsonMap::from_iter(id_and_value)))
            }
            (Literal::List(list), Type::List(list_type)) => Ok(JsonValue::Array(
                list.into_iter()
                    .map(|opt| match opt {
                        Some(literal) => literal.try_into_json(&list_type.element_field.field_type),
                        None => Ok(JsonValue::Null),
                    })
                    .collect::<Result<Vec<JsonValue>>>()?,
            )),
            (Literal::Map(map), Type::Map(map_type)) => {
                let mut object = JsonMap::with_capacity(2);
                let mut json_keys = Vec::with_capacity(map.len());
                let mut json_values = Vec::with_capacity(map.len());
                for (key, value) in map.into_iter() {
                    json_keys.push(key.try_into_json(&map_type.key_field.field_type)?);
                    json_values.push(match value {
                        Some(literal) => literal.try_into_json(&map_type.value_field.field_type)?,
                        None => JsonValue::Null,
                    });
                }
                object.insert("keys".to_string(), JsonValue::Array(json_keys));
                object.insert("values".to_string(), JsonValue::Array(json_values));
                Ok(JsonValue::Object(object))
            }
            (value, r#type) => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("The iceberg value {value:?} doesn't fit to the iceberg type {type}."),
            )),
        }
    }

    /// Convert Value to the any type
    pub fn into_any(self) -> Box<dyn Any> {
        match self {
            Literal::Primitive(prim) => match prim {
                PrimitiveLiteral::Boolean(any) => Box::new(any),
                PrimitiveLiteral::Int(any) => Box::new(any),
                PrimitiveLiteral::Long(any) => Box::new(any),
                PrimitiveLiteral::Float(any) => Box::new(any),
                PrimitiveLiteral::Double(any) => Box::new(any),
                PrimitiveLiteral::Binary(any) => Box::new(any),
                PrimitiveLiteral::String(any) => Box::new(any),
                PrimitiveLiteral::UInt128(any) => Box::new(any),
                PrimitiveLiteral::Int128(any) => Box::new(any),
                PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }
}
