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

//! Typed literals with validation

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use num_bigint::BigInt;
use ordered_float::{Float, OrderedFloat};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::de::{self, MapAccess};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use super::literal::Literal;
use super::primitive::PrimitiveLiteral;
use super::serde::_serde::RawLiteral;
use super::temporal::{date, time, timestamp, timestamptz};
use crate::error::Result;
use crate::spec::MAX_DECIMAL_PRECISION;
use crate::spec::datatypes::{PrimitiveType, Type};
use crate::{Error, ErrorKind, ensure_data_valid};

/// Maximum value for [`PrimitiveType::Time`] type in microseconds, e.g. 23 hours 59 minutes 59 seconds 999999 microseconds.
pub(crate) const MAX_TIME_VALUE: i64 = 24 * 60 * 60 * 1_000_000i64 - 1;

pub(crate) const INT_MAX: i32 = 2147483647;
pub(crate) const INT_MIN: i32 = -2147483648;
pub(crate) const LONG_MAX: i64 = 9223372036854775807;
pub(crate) const LONG_MIN: i64 = -9223372036854775808;

/// Literal associated with its type. The value and type pair is checked when construction, so the type and value is
/// guaranteed to be correct when used.
///
/// By default, we decouple the type and value of a literal, so we can use avoid the cost of storing extra type info
/// for each literal. But associate type with literal can be useful in some cases, for example, in unbound expression.
#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub struct Datum {
    r#type: PrimitiveType,
    literal: PrimitiveLiteral,
}

impl Serialize for Datum {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut struct_ser = serializer
            .serialize_struct("Datum", 2)
            .map_err(serde::ser::Error::custom)?;
        struct_ser
            .serialize_field("type", &self.r#type)
            .map_err(serde::ser::Error::custom)?;
        struct_ser
            .serialize_field(
                "literal",
                &RawLiteral::try_from(
                    Literal::Primitive(self.literal.clone()),
                    &Type::Primitive(self.r#type.clone()),
                )
                .map_err(serde::ser::Error::custom)?,
            )
            .map_err(serde::ser::Error::custom)?;
        struct_ser.end()
    }
}

impl<'de> Deserialize<'de> for Datum {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Type,
            Literal,
        }

        struct DatumVisitor;

        impl<'de> serde::de::Visitor<'de> for DatumVisitor {
            type Value = Datum;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Datum")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where A: serde::de::SeqAccess<'de> {
                let r#type = seq
                    .next_element::<PrimitiveType>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let value = seq
                    .next_element::<RawLiteral>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let Literal::Primitive(primitive) = value
                    .try_into(&Type::Primitive(r#type.clone()))
                    .map_err(serde::de::Error::custom)?
                    .ok_or_else(|| serde::de::Error::custom("None value"))?
                else {
                    return Err(serde::de::Error::custom("Invalid value"));
                };

                Ok(Datum::new(r#type, primitive))
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Datum, V::Error>
            where V: MapAccess<'de> {
                let mut raw_primitive: Option<RawLiteral> = None;
                let mut r#type: Option<PrimitiveType> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Type => {
                            if r#type.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            r#type = Some(map.next_value()?);
                        }
                        Field::Literal => {
                            if raw_primitive.is_some() {
                                return Err(de::Error::duplicate_field("literal"));
                            }
                            raw_primitive = Some(map.next_value()?);
                        }
                    }
                }
                let Some(r#type) = r#type else {
                    return Err(serde::de::Error::missing_field("type"));
                };
                let Some(raw_primitive) = raw_primitive else {
                    return Err(serde::de::Error::missing_field("literal"));
                };
                let Literal::Primitive(primitive) = raw_primitive
                    .try_into(&Type::Primitive(r#type.clone()))
                    .map_err(serde::de::Error::custom)?
                    .ok_or_else(|| serde::de::Error::custom("None value"))?
                else {
                    return Err(serde::de::Error::custom("Invalid value"));
                };
                Ok(Datum::new(r#type, primitive))
            }
        }
        const FIELDS: &[&str] = &["type", "literal"];
        deserializer.deserialize_struct("Datum", FIELDS, DatumVisitor)
    }
}

// Compare following iceberg float ordering rules:
//  -NaN < -Infinity < -value < -0 < 0 < value < Infinity < NaN
fn iceberg_float_cmp_f32(a: OrderedFloat<f32>, b: OrderedFloat<f32>) -> Option<Ordering> {
    Some(a.total_cmp(&b))
}

fn iceberg_float_cmp_f64(a: OrderedFloat<f64>, b: OrderedFloat<f64>) -> Option<Ordering> {
    Some(a.total_cmp(&b))
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (&self.literal, &other.literal, &self.r#type, &other.r#type) {
            // generate the arm with same type and same literal
            (
                PrimitiveLiteral::Boolean(val),
                PrimitiveLiteral::Boolean(other_val),
                PrimitiveType::Boolean,
                PrimitiveType::Boolean,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Int(val),
                PrimitiveLiteral::Int(other_val),
                PrimitiveType::Int,
                PrimitiveType::Int,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Long,
                PrimitiveType::Long,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Float(val),
                PrimitiveLiteral::Float(other_val),
                PrimitiveType::Float,
                PrimitiveType::Float,
            ) => iceberg_float_cmp_f32(*val, *other_val),
            (
                PrimitiveLiteral::Double(val),
                PrimitiveLiteral::Double(other_val),
                PrimitiveType::Double,
                PrimitiveType::Double,
            ) => iceberg_float_cmp_f64(*val, *other_val),
            (
                PrimitiveLiteral::Int(val),
                PrimitiveLiteral::Int(other_val),
                PrimitiveType::Date,
                PrimitiveType::Date,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Time,
                PrimitiveType::Time,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Timestamp,
                PrimitiveType::Timestamp,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Long(val),
                PrimitiveLiteral::Long(other_val),
                PrimitiveType::Timestamptz,
                PrimitiveType::Timestamptz,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::String(val),
                PrimitiveLiteral::String(other_val),
                PrimitiveType::String,
                PrimitiveType::String,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::UInt128(val),
                PrimitiveLiteral::UInt128(other_val),
                PrimitiveType::Uuid,
                PrimitiveType::Uuid,
            ) => uuid::Uuid::from_u128(*val).partial_cmp(&uuid::Uuid::from_u128(*other_val)),
            (
                PrimitiveLiteral::Binary(val),
                PrimitiveLiteral::Binary(other_val),
                PrimitiveType::Fixed(_),
                PrimitiveType::Fixed(_),
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Binary(val),
                PrimitiveLiteral::Binary(other_val),
                PrimitiveType::Binary,
                PrimitiveType::Binary,
            ) => val.partial_cmp(other_val),
            (
                PrimitiveLiteral::Int128(val),
                PrimitiveLiteral::Int128(other_val),
                PrimitiveType::Decimal {
                    precision: _,
                    scale,
                },
                PrimitiveType::Decimal {
                    precision: _,
                    scale: other_scale,
                },
            ) => {
                let val = Decimal::from_i128_with_scale(*val, *scale);
                let other_val = Decimal::from_i128_with_scale(*other_val, *other_scale);
                val.partial_cmp(&other_val)
            }
            _ => None,
        }
    }
}

impl Display for Datum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (&self.r#type, &self.literal) {
            (_, PrimitiveLiteral::Boolean(val)) => write!(f, "{val}"),
            (PrimitiveType::Int, PrimitiveLiteral::Int(val)) => write!(f, "{val}"),
            (PrimitiveType::Long, PrimitiveLiteral::Long(val)) => write!(f, "{val}"),
            (_, PrimitiveLiteral::Float(val)) => write!(f, "{val}"),
            (_, PrimitiveLiteral::Double(val)) => write!(f, "{val}"),
            (PrimitiveType::Date, PrimitiveLiteral::Int(val)) => {
                write!(f, "{}", date::days_to_date(*val))
            }
            (PrimitiveType::Time, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", time::microseconds_to_time(*val))
            }
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", timestamp::microseconds_to_datetime(*val))
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", timestamptz::microseconds_to_datetimetz(*val))
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", timestamp::nanoseconds_to_datetime(*val))
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(val)) => {
                write!(f, "{}", timestamptz::nanoseconds_to_datetimetz(*val))
            }
            (_, PrimitiveLiteral::String(val)) => write!(f, r#""{val}""#),
            (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(val)) => {
                write!(f, "{}", uuid::Uuid::from_u128(*val))
            }
            (_, PrimitiveLiteral::Binary(val)) => display_bytes(val, f),
            (
                PrimitiveType::Decimal {
                    precision: _,
                    scale,
                },
                PrimitiveLiteral::Int128(val),
            ) => {
                write!(f, "{}", Decimal::from_i128_with_scale(*val, *scale))
            }
            (_, _) => {
                unreachable!()
            }
        }
    }
}

fn display_bytes(bytes: &[u8], f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02X}"));
    }
    f.write_str(&s)
}

impl From<Datum> for Literal {
    fn from(value: Datum) -> Self {
        Literal::Primitive(value.literal)
    }
}

impl From<Datum> for PrimitiveLiteral {
    fn from(value: Datum) -> Self {
        value.literal
    }
}

impl Datum {
    /// Creates a `Datum` from a `PrimitiveType` and a `PrimitiveLiteral`
    pub(crate) fn new(r#type: PrimitiveType, literal: PrimitiveLiteral) -> Self {
        Datum { r#type, literal }
    }

    /// Create iceberg value from bytes.
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
    pub fn try_from_bytes(bytes: &[u8], data_type: PrimitiveType) -> Result<Self> {
        let literal = match data_type {
            PrimitiveType::Boolean => {
                if bytes.len() == 1 && bytes[0] == 0u8 {
                    PrimitiveLiteral::Boolean(false)
                } else {
                    PrimitiveLiteral::Boolean(true)
                }
            }
            PrimitiveType::Int => PrimitiveLiteral::Int(i32::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Long => {
                if bytes.len() == 4 {
                    // In the case of an evolved field
                    PrimitiveLiteral::Long(i32::from_le_bytes(bytes.try_into()?) as i64)
                } else {
                    PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
                }
            }
            PrimitiveType::Float => {
                PrimitiveLiteral::Float(OrderedFloat(f32::from_le_bytes(bytes.try_into()?)))
            }
            PrimitiveType::Double => {
                if bytes.len() == 4 {
                    // In the case of an evolved field
                    PrimitiveLiteral::Double(OrderedFloat(
                        f32::from_le_bytes(bytes.try_into()?) as f64
                    ))
                } else {
                    PrimitiveLiteral::Double(OrderedFloat(f64::from_le_bytes(bytes.try_into()?)))
                }
            }
            PrimitiveType::Date => PrimitiveLiteral::Int(i32::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Time => PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?)),
            PrimitiveType::Timestamp => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::Timestamptz => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::TimestampNs => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::TimestamptzNs => {
                PrimitiveLiteral::Long(i64::from_le_bytes(bytes.try_into()?))
            }
            PrimitiveType::String => {
                PrimitiveLiteral::String(std::str::from_utf8(bytes)?.to_string())
            }
            PrimitiveType::Uuid => {
                PrimitiveLiteral::UInt128(u128::from_be_bytes(bytes.try_into()?))
            }
            PrimitiveType::Fixed(_) => PrimitiveLiteral::Binary(Vec::from(bytes)),
            PrimitiveType::Binary => PrimitiveLiteral::Binary(Vec::from(bytes)),
            PrimitiveType::Decimal { .. } => {
                let unscaled_value = BigInt::from_signed_bytes_be(bytes);
                PrimitiveLiteral::Int128(unscaled_value.to_i128().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Can't convert bytes to i128: {bytes:?}"),
                    )
                })?)
            }
        };
        Ok(Datum::new(data_type, literal))
    }

    /// Convert the value to bytes
    ///
    /// See [this spec](https://iceberg.apache.org/spec/#binary-single-value-serialization) for reference.
    pub fn to_bytes(&self) -> Result<ByteBuf> {
        let buf = match &self.literal {
            PrimitiveLiteral::Boolean(val) => {
                if *val {
                    ByteBuf::from([1u8])
                } else {
                    ByteBuf::from([0u8])
                }
            }
            PrimitiveLiteral::Int(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::Long(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::Float(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::Double(val) => ByteBuf::from(val.to_le_bytes()),
            PrimitiveLiteral::String(val) => ByteBuf::from(val.as_bytes()),
            PrimitiveLiteral::UInt128(val) => ByteBuf::from(val.to_be_bytes()),
            PrimitiveLiteral::Binary(val) => ByteBuf::from(val.as_slice()),
            PrimitiveLiteral::Int128(val) => {
                let PrimitiveType::Decimal { precision, .. } = self.r#type else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "PrimitiveLiteral Int128 must be PrimitiveType Decimal but got {}",
                            &self.r#type
                        ),
                    ));
                };

                // It's required by iceberg spec that we must keep the minimum
                // number of bytes for the value
                let Ok(required_bytes) = Type::decimal_required_bytes(precision) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "PrimitiveType Decimal must has valid precision but got {precision}"
                        ),
                    ));
                };

                // The primitive literal is unscaled value.
                let unscaled_value = BigInt::from(*val);
                // Convert into two's-complement byte representation of the BigInt
                // in big-endian byte order.
                let mut bytes = unscaled_value.to_signed_bytes_be();
                // Truncate with required bytes to make sure.
                bytes.truncate(required_bytes as usize);

                ByteBuf::from(bytes)
            }
            PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Cannot convert AboveMax or BelowMin to bytes".to_string(),
                ));
            }
        };

        Ok(buf)
    }

    /// Creates a boolean value.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::bool(true);
    ///
    /// assert_eq!(format!("{}", t), "true".to_string());
    /// assert_eq!(
    ///     Literal::from(t),
    ///     Literal::Primitive(PrimitiveLiteral::Boolean(true))
    /// );
    /// ```
    pub fn bool<T: Into<bool>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Boolean,
            literal: PrimitiveLiteral::Boolean(t.into()),
        }
    }

    /// Creates a boolean value from string.
    /// See [Parse bool from str](https://doc.rust-lang.org/stable/std/primitive.bool.html#impl-FromStr-for-bool) for reference.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::bool_from_str("false").unwrap();
    ///
    /// assert_eq!(&format!("{}", t), "false");
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Boolean(false)),
    ///     t.into()
    /// );
    /// ```
    pub fn bool_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let v = s.as_ref().parse::<bool>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse string to bool.").with_source(e)
        })?;
        Ok(Self::bool(v))
    }

    /// Creates an 32bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::int(23i8);
    ///
    /// assert_eq!(&format!("{}", t), "23");
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Int(23)), t.into());
    /// ```
    pub fn int<T: Into<i32>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Int,
            literal: PrimitiveLiteral::Int(t.into()),
        }
    }

    /// Creates an 64bit integer.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::long(24i8);
    ///
    /// assert_eq!(&format!("{t}"), "24");
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Long(24)), t.into());
    /// ```
    pub fn long<T: Into<i64>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Long,
            literal: PrimitiveLiteral::Long(t.into()),
        }
    }

    /// Creates an 32bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Datum::float(32.1f32);
    ///
    /// assert_eq!(&format!("{t}"), "32.1");
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Float(OrderedFloat(32.1))),
    ///     t.into()
    /// );
    /// ```
    pub fn float<T: Into<f32>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Float,
            literal: PrimitiveLiteral::Float(OrderedFloat(t.into())),
        }
    }

    /// Creates an 64bit floating point number.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// use ordered_float::OrderedFloat;
    /// let t = Datum::double(32.1f64);
    ///
    /// assert_eq!(&format!("{t}"), "32.1");
    /// assert_eq!(
    ///     Literal::Primitive(PrimitiveLiteral::Double(OrderedFloat(32.1))),
    ///     t.into()
    /// );
    /// ```
    pub fn double<T: Into<f64>>(t: T) -> Self {
        Self {
            r#type: PrimitiveType::Double,
            literal: PrimitiveLiteral::Double(OrderedFloat(t.into())),
        }
    }

    /// Creates date literal from number of days from unix epoch directly.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// // 2 days after 1970-01-01
    /// let t = Datum::date(2);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-03");
    /// assert_eq!(Literal::Primitive(PrimitiveLiteral::Int(2)), t.into());
    /// ```
    pub fn date(days: i32) -> Self {
        Self {
            r#type: PrimitiveType::Date,
            literal: PrimitiveLiteral::Int(days),
        }
    }

    /// Creates date literal in `%Y-%m-%d` format, assume in utc timezone.
    ///
    /// See [`NaiveDate::from_str`].
    ///
    /// Example
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::date_from_str("1970-01-05").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-05");
    /// assert_eq!(Literal::date(4), t.into());
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

    /// Create date literal from calendar date (year, month and day).
    ///
    /// See [`NaiveDate::from_ymd_opt`].
    ///
    /// Example:
    ///
    ///```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::date_from_ymd(1970, 1, 5).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-05");
    /// assert_eq!(Literal::date(4), t.into());
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

    /// Creates time literal in microseconds directly.
    ///
    /// It will return error when it's negative or too large to fit in 24 hours.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let micro_secs = {
    ///     1 * 3600 * 1_000_000 + // 1 hour
    ///     2 * 60 * 1_000_000 +   // 2 minutes
    ///     1 * 1_000_000 + // 1 second
    ///     888999 // microseconds
    /// };
    ///
    /// let t = Datum::time_micros(micro_secs).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "01:02:01.888999");
    /// assert_eq!(Literal::time(micro_secs), t.into());
    ///
    /// let negative_value = -100;
    /// assert!(Datum::time_micros(negative_value).is_err());
    ///
    /// let too_large_value = 36 * 60 * 60 * 1_000_000; // Too large to fit in 24 hours.
    /// assert!(Datum::time_micros(too_large_value).is_err());
    /// ```
    pub fn time_micros(value: i64) -> Result<Self> {
        ensure_data_valid!(
            (0..=MAX_TIME_VALUE).contains(&value),
            "Invalid value for Time type: {}",
            value
        );

        Ok(Self {
            r#type: PrimitiveType::Time,
            literal: PrimitiveLiteral::Long(value),
        })
    }

    /// Creates time literal from [`chrono::NaiveTime`].
    fn time_from_naive_time(t: NaiveTime) -> Self {
        let duration = t - date::unix_epoch().time();
        // It's safe to unwrap here since less than 24 hours will never overflow.
        let micro_secs = duration.num_microseconds().unwrap();

        Self {
            r#type: PrimitiveType::Time,
            literal: PrimitiveLiteral::Long(micro_secs),
        }
    }

    /// Creates time literal in microseconds in `%H:%M:%S:.f` format.
    ///
    /// See [`NaiveTime::from_str`] for details.
    ///
    /// Example:
    /// ```rust
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::time_from_str("01:02:01.888999777").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "01:02:01.888999");
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
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::time_from_hms_micro(22, 15, 33, 111).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "22:15:33.000111");
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
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamp_micros(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.001");
    /// ```
    pub fn timestamp_micros(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::Timestamp,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp from unix epoch in nanoseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamp_nanos(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.000001");
    /// ```
    pub fn timestamp_nanos(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::TimestampNs,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp from [`DateTime`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamp_from_datetime(
    ///     NaiveDate::from_ymd_opt(1992, 3, 1)
    ///         .unwrap()
    ///         .and_hms_micro_opt(1, 2, 3, 88)
    ///         .unwrap(),
    /// );
    ///
    /// assert_eq!(&format!("{t}"), "1992-03-01 01:02:03.000088");
    /// ```
    pub fn timestamp_from_datetime(dt: NaiveDateTime) -> Self {
        Self::timestamp_micros(dt.and_utc().timestamp_micros())
    }

    /// Parse a timestamp in [`%Y-%m-%dT%H:%M:%S%.f`] format.
    ///
    /// See [`NaiveDateTime::from_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::timestamp_from_str("1992-03-01T01:02:03.000088").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1992-03-01 01:02:03.000088");
    /// ```
    pub fn timestamp_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = s.as_ref().parse::<NaiveDateTime>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse timestamp.").with_source(e)
        })?;

        Ok(Self::timestamp_from_datetime(dt))
    }

    /// Creates a timestamp with timezone from unix epoch in microseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamptz_micros(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.001 UTC");
    /// ```
    pub fn timestamptz_micros(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::Timestamptz,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp with timezone from unix epoch in nanoseconds.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamptz_nanos(1000);
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:00:00.000001 UTC");
    /// ```
    pub fn timestamptz_nanos(value: i64) -> Self {
        Self {
            r#type: PrimitiveType::TimestamptzNs,
            literal: PrimitiveLiteral::Long(value),
        }
    }

    /// Creates a timestamp with timezone from [`DateTime`].
    /// Example:
    ///
    /// ```rust
    /// use chrono::{TimeZone, Utc};
    /// use iceberg::spec::Datum;
    /// let t = Datum::timestamptz_from_datetime(Utc.timestamp_opt(1000, 0).unwrap());
    ///
    /// assert_eq!(&format!("{t}"), "1970-01-01 00:16:40 UTC");
    /// ```
    pub fn timestamptz_from_datetime<T: TimeZone>(dt: DateTime<T>) -> Self {
        Self::timestamptz_micros(dt.with_timezone(&Utc).timestamp_micros())
    }

    /// Parse timestamp with timezone in RFC3339 format.
    ///
    /// See [`DateTime::from_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
    /// use iceberg::spec::{Datum, Literal};
    /// let t = Datum::timestamptz_from_str("1992-03-01T01:02:03.000088+08:00").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1992-02-29 17:02:03.000088 UTC");
    /// ```
    pub fn timestamptz_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let dt = DateTime::<Utc>::from_str(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse datetime.").with_source(e)
        })?;

        Ok(Self::timestamptz_from_datetime(dt))
    }

    /// Creates a string literal.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::string("ss");
    ///
    /// assert_eq!(&format!("{t}"), r#""ss""#);
    /// ```
    pub fn string<S: ToString>(s: S) -> Self {
        Self {
            r#type: PrimitiveType::String,
            literal: PrimitiveLiteral::String(s.to_string()),
        }
    }

    /// Creates uuid literal.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use uuid::uuid;
    /// let t = Datum::uuid(uuid!("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"));
    ///
    /// assert_eq!(&format!("{t}"), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
    /// ```
    pub fn uuid(uuid: uuid::Uuid) -> Self {
        Self {
            r#type: PrimitiveType::Uuid,
            literal: PrimitiveLiteral::UInt128(uuid.as_u128()),
        }
    }

    /// Creates uuid from str. See [`uuid::Uuid::parse_str`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::uuid_from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
    /// ```
    pub fn uuid_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let uuid = uuid::Uuid::parse_str(s.as_ref()).map_err(|e| {
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
    /// use iceberg::spec::{Datum, Literal, PrimitiveLiteral};
    /// let t = Datum::fixed(vec![1u8, 2u8]);
    ///
    /// assert_eq!(&format!("{t}"), "0102");
    /// ```
    pub fn fixed<I: IntoIterator<Item = u8>>(input: I) -> Self {
        let value: Vec<u8> = input.into_iter().collect();
        Self {
            r#type: PrimitiveType::Fixed(value.len() as u64),
            literal: PrimitiveLiteral::Binary(value),
        }
    }

    /// Creates a binary literal from bytes.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// let t = Datum::binary(vec![1u8, 100u8]);
    ///
    /// assert_eq!(&format!("{t}"), "0164");
    /// ```
    pub fn binary<I: IntoIterator<Item = u8>>(input: I) -> Self {
        Self {
            r#type: PrimitiveType::Binary,
            literal: PrimitiveLiteral::Binary(input.into_iter().collect()),
        }
    }

    /// Creates decimal literal from string. See [`Decimal::from_str_exact`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use itertools::assert_equal;
    /// use rust_decimal::Decimal;
    /// let t = Datum::decimal_from_str("123.45").unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "123.45");
    /// ```
    pub fn decimal_from_str<S: AsRef<str>>(s: S) -> Result<Self> {
        let decimal = Decimal::from_str_exact(s.as_ref()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse decimal.").with_source(e)
        })?;

        Self::decimal(decimal)
    }

    /// Try to create a decimal literal from [`Decimal`].
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use rust_decimal::Decimal;
    ///
    /// let t = Datum::decimal(Decimal::new(123, 2)).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1.23");
    /// ```
    pub fn decimal(value: impl Into<Decimal>) -> Result<Self> {
        let decimal = value.into();
        let scale = decimal.scale();

        let r#type = Type::decimal(MAX_DECIMAL_PRECISION, scale)?;
        if let Type::Primitive(p) = r#type {
            Ok(Self {
                r#type: p,
                literal: PrimitiveLiteral::Int128(decimal.mantissa()),
            })
        } else {
            unreachable!("Decimal type must be primitive.")
        }
    }

    /// Try to create a decimal literal from [`Decimal`] with precision.
    ///
    /// Example:
    ///
    /// ```rust
    /// use iceberg::spec::Datum;
    /// use rust_decimal::Decimal;
    ///
    /// let t = Datum::decimal_with_precision(Decimal::new(123, 2), 30).unwrap();
    ///
    /// assert_eq!(&format!("{t}"), "1.23");
    /// ```
    pub fn decimal_with_precision(value: impl Into<Decimal>, precision: u32) -> Result<Self> {
        let decimal = value.into();
        let scale = decimal.scale();

        let available_bytes = Type::decimal_required_bytes(precision)? as usize;
        let unscaled_value = BigInt::from(decimal.mantissa());
        let actual_bytes = unscaled_value.to_signed_bytes_be();
        if actual_bytes.len() > available_bytes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Decimal value {decimal} is too large for precision {precision}"),
            ));
        }

        let r#type = Type::decimal(precision, scale)?;
        if let Type::Primitive(p) = r#type {
            Ok(Self {
                r#type: p,
                literal: PrimitiveLiteral::Int128(decimal.mantissa()),
            })
        } else {
            unreachable!("Decimal type must be primitive.")
        }
    }

    fn i64_to_i32<T: Into<i64> + PartialOrd<i64>>(val: T) -> Datum {
        if val > INT_MAX as i64 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::AboveMax)
        } else if val < INT_MIN as i64 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::BelowMin)
        } else {
            Datum::int(val.into() as i32)
        }
    }

    fn i128_to_i32<T: Into<i128> + PartialOrd<i128>>(val: T) -> Datum {
        if val > INT_MAX as i128 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::AboveMax)
        } else if val < INT_MIN as i128 {
            Datum::new(PrimitiveType::Int, PrimitiveLiteral::BelowMin)
        } else {
            Datum::int(val.into() as i32)
        }
    }

    fn i128_to_i64<T: Into<i128> + PartialOrd<i128>>(val: T) -> Datum {
        if val > LONG_MAX as i128 {
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::AboveMax)
        } else if val < LONG_MIN as i128 {
            Datum::new(PrimitiveType::Long, PrimitiveLiteral::BelowMin)
        } else {
            Datum::long(val.into() as i64)
        }
    }

    fn string_to_i128<S: AsRef<str>>(s: S) -> Result<i128> {
        s.as_ref().parse::<i128>().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Can't parse string to i128.").with_source(e)
        })
    }

    /// Convert the datum to `target_type`.
    pub fn to(self, target_type: &Type) -> Result<Datum> {
        match target_type {
            Type::Primitive(target_primitive_type) => {
                match (&self.literal, &self.r#type, target_primitive_type) {
                    (PrimitiveLiteral::Int(val), _, PrimitiveType::Int) => Ok(Datum::int(*val)),
                    (PrimitiveLiteral::Int(val), _, PrimitiveType::Date) => Ok(Datum::date(*val)),
                    (PrimitiveLiteral::Int(val), _, PrimitiveType::Long) => Ok(Datum::long(*val)),
                    (PrimitiveLiteral::Long(val), _, PrimitiveType::Int) => {
                        Ok(Datum::i64_to_i32(*val))
                    }
                    (PrimitiveLiteral::Long(val), _, PrimitiveType::Timestamp) => {
                        Ok(Datum::timestamp_micros(*val))
                    }
                    (PrimitiveLiteral::Long(val), _, PrimitiveType::Timestamptz) => {
                        Ok(Datum::timestamptz_micros(*val))
                    }
                    // Let's wait with nano's until this clears up: https://github.com/apache/iceberg/pull/11775
                    (PrimitiveLiteral::Int128(val), _, PrimitiveType::Long) => {
                        Ok(Datum::i128_to_i64(*val))
                    }

                    (PrimitiveLiteral::String(val), _, PrimitiveType::Boolean) => {
                        Datum::bool_from_str(val)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Int) => {
                        Datum::string_to_i128(val).map(Datum::i128_to_i32)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Long) => {
                        Datum::string_to_i128(val).map(Datum::i128_to_i64)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Timestamp) => {
                        Datum::timestamp_from_str(val)
                    }
                    (PrimitiveLiteral::String(val), _, PrimitiveType::Timestamptz) => {
                        Datum::timestamptz_from_str(val)
                    }

                    // TODO: implement more type conversions
                    (_, self_type, target_type) if self_type == target_type => Ok(self),
                    _ => Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Can't convert datum from {} type to {} type.",
                            self.r#type, target_primitive_type
                        ),
                    )),
                }
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Can't convert datum from {} type to {} type.",
                    self.r#type, target_type
                ),
            )),
        }
    }

    /// Get the primitive literal from datum.
    pub fn literal(&self) -> &PrimitiveLiteral {
        &self.literal
    }

    /// Get the primitive type from datum.
    pub fn data_type(&self) -> &PrimitiveType {
        &self.r#type
    }

    /// Returns true if the Literal represents a primitive type
    /// that can be a NaN, and that it's value is NaN
    pub fn is_nan(&self) -> bool {
        match self.literal {
            PrimitiveLiteral::Double(val) => val.is_nan(),
            PrimitiveLiteral::Float(val) => val.is_nan(),
            _ => false,
        }
    }

    /// Returns a human-readable string representation of this literal.
    ///
    /// For string literals, this returns the raw string value without quotes.
    /// For all other literals, it falls back to [`to_string()`].
    pub fn to_human_string(&self) -> String {
        match self.literal() {
            PrimitiveLiteral::String(s) => s.to_string(),
            _ => self.to_string(),
        }
    }
}
