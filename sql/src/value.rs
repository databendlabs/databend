// Copyright 2021 Datafuse Labs
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

use arrow::datatypes::{i256, ArrowNativeTypeOp};
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::{
    error::{ConvertError, Error, Result},
    schema::{DecimalDataType, DecimalSize},
};

use geozero::wkb::FromWkb;
use geozero::wkb::WkbDialect;
use geozero::wkt::Ewkt;
use std::fmt::Write;

// Thu 1970-01-01 is R.D. 719163
const DAYS_FROM_CE: i32 = 719_163;
const NULL_VALUE: &str = "NULL";

#[cfg(feature = "flight-sql")]
use {
    crate::schema::{
        ARROW_EXT_TYPE_BITMAP, ARROW_EXT_TYPE_EMPTY_ARRAY, ARROW_EXT_TYPE_EMPTY_MAP,
        ARROW_EXT_TYPE_GEOMETRY, ARROW_EXT_TYPE_VARIANT, EXTENSION_KEY,
    },
    arrow_array::{
        Array as ArrowArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        Decimal256Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, StringArray,
        StructArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    arrow_schema::{DataType as ArrowDataType, Field as ArrowField, TimeUnit},
    std::sync::Arc,
};

use crate::schema::{DataType, NumberDataType};

#[derive(Clone, Debug, PartialEq)]
pub enum NumberValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal128(i128, DecimalSize),
    Decimal256(i256, DecimalSize),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean(bool),
    Binary(Vec<u8>),
    String(String),
    Number(NumberValue),
    /// Microseconds from 1970-01-01 00:00:00 UTC
    Timestamp(i64),
    Date(i32),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Tuple(Vec<Value>),
    Bitmap(String),
    Variant(String),
    Geometry(String),
}

impl Value {
    pub fn get_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::EmptyArray => DataType::EmptyArray,
            Self::EmptyMap => DataType::EmptyMap,
            Self::Boolean(_) => DataType::Boolean,
            Self::Binary(_) => DataType::Binary,
            Self::String(_) => DataType::String,
            Self::Number(n) => match n {
                NumberValue::Int8(_) => DataType::Number(NumberDataType::Int8),
                NumberValue::Int16(_) => DataType::Number(NumberDataType::Int16),
                NumberValue::Int32(_) => DataType::Number(NumberDataType::Int32),
                NumberValue::Int64(_) => DataType::Number(NumberDataType::Int64),
                NumberValue::UInt8(_) => DataType::Number(NumberDataType::UInt8),
                NumberValue::UInt16(_) => DataType::Number(NumberDataType::UInt16),
                NumberValue::UInt32(_) => DataType::Number(NumberDataType::UInt32),
                NumberValue::UInt64(_) => DataType::Number(NumberDataType::UInt64),
                NumberValue::Float32(_) => DataType::Number(NumberDataType::Float32),
                NumberValue::Float64(_) => DataType::Number(NumberDataType::Float64),
                NumberValue::Decimal128(_, s) => DataType::Decimal(DecimalDataType::Decimal128(*s)),
                NumberValue::Decimal256(_, s) => DataType::Decimal(DecimalDataType::Decimal256(*s)),
            },
            Self::Timestamp(_) => DataType::Timestamp,

            Self::Date(_) => DataType::Date,
            Self::Array(vals) => {
                if vals.is_empty() {
                    DataType::EmptyArray
                } else {
                    DataType::Array(Box::new(vals[0].get_type()))
                }
            }
            Self::Map(kvs) => {
                if kvs.is_empty() {
                    DataType::EmptyMap
                } else {
                    let inner_ty = DataType::Tuple(vec![kvs[0].0.get_type(), kvs[0].1.get_type()]);
                    DataType::Map(Box::new(inner_ty))
                }
            }
            Self::Tuple(vals) => {
                let inner_tys = vals.iter().map(|v| v.get_type()).collect::<Vec<_>>();
                DataType::Tuple(inner_tys)
            }
            Self::Bitmap(_) => DataType::Bitmap,
            Self::Variant(_) => DataType::Variant,
            Self::Geometry(_) => DataType::Geometry,
        }
    }
}

impl TryFrom<(&DataType, &str)> for Value {
    type Error = Error;

    fn try_from((t, v): (&DataType, &str)) -> Result<Self> {
        match t {
            DataType::Null => Ok(Self::Null),
            DataType::EmptyArray => Ok(Self::EmptyArray),
            DataType::EmptyMap => Ok(Self::EmptyMap),
            DataType::Boolean => Ok(Self::Boolean(v == "1")),
            DataType::Binary => Ok(Self::Binary(hex::decode(v)?)),
            DataType::String => Ok(Self::String(v.to_string())),

            DataType::Number(NumberDataType::Int8) => {
                Ok(Self::Number(NumberValue::Int8(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int16) => {
                Ok(Self::Number(NumberValue::Int16(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int32) => {
                Ok(Self::Number(NumberValue::Int32(v.parse()?)))
            }
            DataType::Number(NumberDataType::Int64) => {
                Ok(Self::Number(NumberValue::Int64(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt8) => {
                Ok(Self::Number(NumberValue::UInt8(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt16) => {
                Ok(Self::Number(NumberValue::UInt16(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt32) => {
                Ok(Self::Number(NumberValue::UInt32(v.parse()?)))
            }
            DataType::Number(NumberDataType::UInt64) => {
                Ok(Self::Number(NumberValue::UInt64(v.parse()?)))
            }
            DataType::Number(NumberDataType::Float32) => {
                Ok(Self::Number(NumberValue::Float32(v.parse()?)))
            }
            DataType::Number(NumberDataType::Float64) => {
                Ok(Self::Number(NumberValue::Float64(v.parse()?)))
            }

            DataType::Decimal(DecimalDataType::Decimal128(size)) => {
                let d = parse_decimal(v, *size)?;
                Ok(Self::Number(d))
            }
            DataType::Decimal(DecimalDataType::Decimal256(size)) => {
                let d = parse_decimal(v, *size)?;
                Ok(Self::Number(d))
            }

            DataType::Timestamp => Ok(Self::Timestamp(
                chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.6f")?
                    .timestamp_micros(),
            )),
            DataType::Date => Ok(Self::Date(
                chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d")?.num_days_from_ce() - DAYS_FROM_CE,
            )),
            DataType::Bitmap => Ok(Self::Bitmap(v.to_string())),
            DataType::Variant => Ok(Self::Variant(v.to_string())),
            DataType::Geometry => Ok(Self::Geometry(v.to_string())),

            DataType::Nullable(inner) => {
                if v == NULL_VALUE {
                    Ok(Self::Null)
                } else {
                    Self::try_from((inner.as_ref(), v))
                }
            }

            // TODO:(everpcpc) handle complex types
            _ => Ok(Self::String(v.to_string())),
        }
    }
}

#[cfg(feature = "flight-sql")]
impl TryFrom<(&ArrowField, &Arc<dyn ArrowArray>, usize)> for Value {
    type Error = Error;
    fn try_from(
        (field, array, seq): (&ArrowField, &Arc<dyn ArrowArray>, usize),
    ) -> std::result::Result<Self, Self::Error> {
        if let Some(extend_type) = field.metadata().get(EXTENSION_KEY) {
            match extend_type.as_str() {
                ARROW_EXT_TYPE_EMPTY_ARRAY => {
                    return Ok(Value::EmptyArray);
                }
                ARROW_EXT_TYPE_EMPTY_MAP => {
                    return Ok(Value::EmptyMap);
                }
                ARROW_EXT_TYPE_VARIANT => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    return match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => Ok(Value::Variant(jsonb::to_string(array.value(seq)))),
                        None => Err(ConvertError::new("variant", format!("{:?}", array)).into()),
                    };
                }
                ARROW_EXT_TYPE_BITMAP => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    return match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => {
                            let rb = roaring::RoaringTreemap::deserialize_from(array.value(seq))
                                .expect("failed to deserialize bitmap");
                            let raw = rb.into_iter().collect::<Vec<_>>();
                            let s = itertools::join(raw.iter(), ",");
                            Ok(Value::Bitmap(s))
                        }
                        None => Err(ConvertError::new("bitmap", format!("{:?}", array)).into()),
                    };
                }
                ARROW_EXT_TYPE_GEOMETRY => {
                    if field.is_nullable() && array.is_null(seq) {
                        return Ok(Value::Null);
                    }
                    return match array.as_any().downcast_ref::<LargeBinaryArray>() {
                        Some(array) => {
                            let wkt = parse_geometry(array.value(seq))?;
                            Ok(Value::Geometry(wkt))
                        }
                        None => Err(ConvertError::new("geometry", format!("{:?}", array)).into()),
                    };
                }
                _ => {
                    return Err(ConvertError::new(
                        "extension",
                        format!(
                            "Unsupported extension datatype for arrow field: {:?}",
                            field
                        ),
                    )
                    .into());
                }
            }
        }

        if field.is_nullable() && array.is_null(seq) {
            return Ok(Value::Null);
        }
        match field.data_type() {
            ArrowDataType::Null => Ok(Value::Null),
            ArrowDataType::Boolean => match array.as_any().downcast_ref::<BooleanArray>() {
                Some(array) => Ok(Value::Boolean(array.value(seq))),
                None => Err(ConvertError::new("bool", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int8 => match array.as_any().downcast_ref::<Int8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int8(array.value(seq)))),
                None => Err(ConvertError::new("int8", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int16 => match array.as_any().downcast_ref::<Int16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int16(array.value(seq)))),
                None => Err(ConvertError::new("int16", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int32 => match array.as_any().downcast_ref::<Int32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int32(array.value(seq)))),
                None => Err(ConvertError::new("int64", format!("{:?}", array)).into()),
            },
            ArrowDataType::Int64 => match array.as_any().downcast_ref::<Int64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Int64(array.value(seq)))),
                None => Err(ConvertError::new("int64", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt8 => match array.as_any().downcast_ref::<UInt8Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt8(array.value(seq)))),
                None => Err(ConvertError::new("uint8", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt16 => match array.as_any().downcast_ref::<UInt16Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt16(array.value(seq)))),
                None => Err(ConvertError::new("uint16", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt32 => match array.as_any().downcast_ref::<UInt32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt32(array.value(seq)))),
                None => Err(ConvertError::new("uint32", format!("{:?}", array)).into()),
            },
            ArrowDataType::UInt64 => match array.as_any().downcast_ref::<UInt64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::UInt64(array.value(seq)))),
                None => Err(ConvertError::new("uint64", format!("{:?}", array)).into()),
            },
            ArrowDataType::Float32 => match array.as_any().downcast_ref::<Float32Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float32(array.value(seq)))),
                None => Err(ConvertError::new("float32", format!("{:?}", array)).into()),
            },
            ArrowDataType::Float64 => match array.as_any().downcast_ref::<Float64Array>() {
                Some(array) => Ok(Value::Number(NumberValue::Float64(array.value(seq)))),
                None => Err(ConvertError::new("float64", format!("{:?}", array)).into()),
            },

            ArrowDataType::Decimal128(p, s) => {
                match array.as_any().downcast_ref::<Decimal128Array>() {
                    Some(array) => Ok(Value::Number(NumberValue::Decimal128(
                        array.value(seq),
                        DecimalSize {
                            precision: *p,
                            scale: *s as u8,
                        },
                    ))),
                    None => Err(ConvertError::new("Decimal128", format!("{:?}", array)).into()),
                }
            }
            ArrowDataType::Decimal256(p, s) => {
                match array.as_any().downcast_ref::<Decimal256Array>() {
                    Some(array) => Ok(Value::Number(NumberValue::Decimal256(
                        array.value(seq),
                        DecimalSize {
                            precision: *p,
                            scale: *s as u8,
                        },
                    ))),
                    None => Err(ConvertError::new("Decimal256", format!("{:?}", array)).into()),
                }
            }

            ArrowDataType::Binary => match array.as_any().downcast_ref::<BinaryArray>() {
                Some(array) => Ok(Value::String(String::from_utf8(array.value(seq).to_vec())?)),
                None => Err(ConvertError::new("binary", format!("{:?}", array)).into()),
            },
            ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => {
                match array.as_any().downcast_ref::<LargeBinaryArray>() {
                    Some(array) => Ok(Value::String(String::from_utf8(array.value(seq).to_vec())?)),
                    None => Err(ConvertError::new("large binary", format!("{:?}", array)).into()),
                }
            }
            ArrowDataType::Utf8 => match array.as_any().downcast_ref::<StringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("string", format!("{:?}", array)).into()),
            },
            ArrowDataType::LargeUtf8 => match array.as_any().downcast_ref::<LargeStringArray>() {
                Some(array) => Ok(Value::String(array.value(seq).to_string())),
                None => Err(ConvertError::new("large string", format!("{:?}", array)).into()),
            },

            // we only support timestamp in microsecond in databend
            ArrowDataType::Timestamp(unit, tz) => {
                match array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    Some(array) => {
                        if unit != &TimeUnit::Microsecond {
                            return Err(ConvertError::new("timestamp", format!("{:?}", array))
                                .with_message(format!(
                                    "unsupported timestamp unit: {:?}, only support microsecond",
                                    unit
                                ))
                                .into());
                        }
                        let ts = array.value(seq);
                        match tz {
                            None => Ok(Value::Timestamp(ts)),
                            Some(tz) => Err(ConvertError::new("timestamp", format!("{:?}", array))
                                .with_message(format!("non-UTC timezone not supported: {:?}", tz))
                                .into()),
                        }
                    }
                    None => Err(ConvertError::new("timestamp", format!("{:?}", array)).into()),
                }
            }
            ArrowDataType::Date32 => match array.as_any().downcast_ref::<Date32Array>() {
                Some(array) => Ok(Value::Date(array.value(seq))),
                None => Err(ConvertError::new("date", format!("{:?}", array)).into()),
            },
            ArrowDataType::List(f) => match array.as_any().downcast_ref::<ListArray>() {
                Some(array) => {
                    let inner_array = unsafe { array.value_unchecked(seq) };
                    let mut values = Vec::with_capacity(inner_array.len());
                    for i in 0..inner_array.len() {
                        let value = Value::try_from((f.as_ref(), &inner_array, i))?;
                        values.push(value);
                    }
                    Ok(Value::Array(values))
                }
                None => Err(ConvertError::new("list", format!("{:?}", array)).into()),
            },
            ArrowDataType::LargeList(f) => match array.as_any().downcast_ref::<LargeListArray>() {
                Some(array) => {
                    let inner_array = unsafe { array.value_unchecked(seq) };
                    let mut values = Vec::with_capacity(inner_array.len());
                    for i in 0..inner_array.len() {
                        let value = Value::try_from((f.as_ref(), &inner_array, i))?;
                        values.push(value);
                    }
                    Ok(Value::Array(values))
                }
                None => Err(ConvertError::new("large list", format!("{:?}", array)).into()),
            },
            ArrowDataType::Map(f, _) => match array.as_any().downcast_ref::<MapArray>() {
                Some(array) => {
                    if let ArrowDataType::Struct(fs) = f.data_type() {
                        let inner_array = unsafe { array.value_unchecked(seq) };
                        let mut values = Vec::with_capacity(inner_array.len());
                        for i in 0..inner_array.len() {
                            let key = Value::try_from((fs[0].as_ref(), inner_array.column(0), i))?;
                            let val = Value::try_from((fs[1].as_ref(), inner_array.column(1), i))?;
                            values.push((key, val));
                        }
                        Ok(Value::Map(values))
                    } else {
                        Err(
                            ConvertError::new("invalid map inner type", format!("{:?}", array))
                                .into(),
                        )
                    }
                }
                None => Err(ConvertError::new("map", format!("{:?}", array)).into()),
            },
            ArrowDataType::Struct(fs) => match array.as_any().downcast_ref::<StructArray>() {
                Some(array) => {
                    let mut values = Vec::with_capacity(array.len());
                    for (f, inner_array) in fs.iter().zip(array.columns().iter()) {
                        let value = Value::try_from((f.as_ref(), inner_array, seq))?;
                        values.push(value);
                    }
                    Ok(Value::Tuple(values))
                }
                None => Err(ConvertError::new("struct", format!("{:?}", array)).into()),
            },
            _ => Err(ConvertError::new("unsupported data type", format!("{:?}", array)).into()),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::String(s) => Ok(s),
            Value::Bitmap(s) => Ok(s),
            Value::Variant(s) => Ok(s),
            _ => Err(ConvertError::new("string", format!("{:?}", val)).into()),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Boolean(b) => Ok(b),
            Value::Number(n) => Ok(n != NumberValue::Int8(0)),
            _ => Err(ConvertError::new("bool", format!("{:?}", val)).into()),
        }
    }
}

// This macro implements TryFrom for NumberValue
macro_rules! impl_try_from_number_value {
    ($($t:ty),*) => {
        $(
            impl TryFrom<Value> for $t {
                type Error = Error;
                fn try_from(val: Value) -> Result<Self> {
                    match val {
                        Value::Number(NumberValue::Int8(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int16(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Int64(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt8(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt16(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::UInt64(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Float32(i)) => Ok(i as $t),
                        Value::Number(NumberValue::Float64(i)) => Ok(i as $t),
                        Value::Date(i) => Ok(i as $t),
                        Value::Timestamp(i) => Ok(i as $t),
                        _ => Err(ConvertError::new("number", format!("{:?}", val)).into()),
                    }
                }
            }
        )*
    };
}

impl_try_from_number_value!(u8);
impl_try_from_number_value!(u16);
impl_try_from_number_value!(u32);
impl_try_from_number_value!(u64);
impl_try_from_number_value!(i8);
impl_try_from_number_value!(i16);
impl_try_from_number_value!(i32);
impl_try_from_number_value!(i64);
impl_try_from_number_value!(f32);
impl_try_from_number_value!(f64);

impl TryFrom<Value> for NaiveDateTime {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Timestamp(i) => {
                let secs = i / 1_000_000;
                let nanos = ((i % 1_000_000) * 1000) as u32;
                let t = NaiveDateTime::from_timestamp_opt(secs, nanos);
                match t {
                    Some(t) => Ok(t),
                    None => Err(ConvertError::new("NaiveDateTime", "".to_string()).into()),
                }
            }
            _ => Err(ConvertError::new("NaiveDateTime", format!("{}", val)).into()),
        }
    }
}

impl TryFrom<Value> for NaiveDate {
    type Error = Error;
    fn try_from(val: Value) -> Result<Self> {
        match val {
            Value::Date(i) => {
                let days = i + DAYS_FROM_CE;
                let d = NaiveDate::from_num_days_from_ce_opt(days);
                match d {
                    Some(d) => Ok(d),
                    None => Err(ConvertError::new("NaiveDate", "".to_string()).into()),
                }
            }
            _ => Err(ConvertError::new("NaiveDate", format!("{}", val)).into()),
        }
    }
}

// This macro implements TryFrom to Option for Nullable column
macro_rules! impl_try_from_to_option {
    ($($t:ty),*) => {
        $(
            impl TryFrom<Value> for Option<$t> {
                type Error = Error;
                fn try_from(val: Value) -> Result<Self> {
                    match val {
                        Value::Null => Ok(None),
                        _ => {
                            let inner: $t = val.try_into()?;
                            Ok(Some(inner))
                        },
                    }

                }
            }
        )*
    };
}

impl_try_from_to_option!(String);
impl_try_from_to_option!(bool);
impl_try_from_to_option!(u8);
impl_try_from_to_option!(u16);
impl_try_from_to_option!(u32);
impl_try_from_to_option!(u64);
impl_try_from_to_option!(i8);
impl_try_from_to_option!(i16);
impl_try_from_to_option!(i32);
impl_try_from_to_option!(i64);
impl_try_from_to_option!(f32);
impl_try_from_to_option!(f64);
impl_try_from_to_option!(NaiveDateTime);
impl_try_from_to_option!(NaiveDate);

impl std::fmt::Display for NumberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberValue::Int8(i) => write!(f, "{}", i),
            NumberValue::Int16(i) => write!(f, "{}", i),
            NumberValue::Int32(i) => write!(f, "{}", i),
            NumberValue::Int64(i) => write!(f, "{}", i),
            NumberValue::UInt8(i) => write!(f, "{}", i),
            NumberValue::UInt16(i) => write!(f, "{}", i),
            NumberValue::UInt32(i) => write!(f, "{}", i),
            NumberValue::UInt64(i) => write!(f, "{}", i),
            NumberValue::Float32(i) => write!(f, "{}", i),
            NumberValue::Float64(i) => write!(f, "{}", i),
            NumberValue::Decimal128(v, s) => write!(f, "{}", display_decimal_128(*v, s.scale)),
            NumberValue::Decimal256(v, s) => write!(f, "{}", display_decimal_256(*v, s.scale)),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        encode_value(f, self, true)
    }
}

// Compatible with Databend, inner values of nested types are quoted.
fn encode_value(f: &mut std::fmt::Formatter<'_>, val: &Value, raw: bool) -> std::fmt::Result {
    match val {
        Value::Null => write!(f, "NULL"),
        Value::EmptyArray => write!(f, "[]"),
        Value::EmptyMap => write!(f, "{{}}"),
        Value::Boolean(b) => write!(f, "{}", b),
        Value::Number(n) => write!(f, "{}", n),
        Value::Binary(s) => write!(f, "{}", hex::encode_upper(s)),
        Value::String(s) | Value::Bitmap(s) | Value::Variant(s) | Value::Geometry(s) => {
            if raw {
                write!(f, "{}", s)
            } else {
                write!(f, "'{}'", s)
            }
        }
        Value::Timestamp(i) => {
            let secs = i / 1_000_000;
            let nanos = ((i % 1_000_000) * 1000) as u32;
            let t = NaiveDateTime::from_timestamp_opt(secs, nanos).unwrap_or_default();
            if raw {
                write!(f, "{}", t)
            } else {
                write!(f, "'{}'", t)
            }
        }
        Value::Date(i) => {
            let days = i + DAYS_FROM_CE;
            let d = NaiveDate::from_num_days_from_ce_opt(days).unwrap_or_default();
            if raw {
                write!(f, "{}", d)
            } else {
                write!(f, "'{}'", d)
            }
        }
        Value::Array(vals) => {
            write!(f, "[")?;
            for (i, val) in vals.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                encode_value(f, val, false)?;
            }
            write!(f, "]")?;
            Ok(())
        }
        Value::Map(kvs) => {
            write!(f, "{{")?;
            for (i, (key, val)) in kvs.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                encode_value(f, key, false)?;
                write!(f, ":")?;
                encode_value(f, val, false)?;
            }
            write!(f, "}}")?;
            Ok(())
        }
        Value::Tuple(vals) => {
            write!(f, "(")?;
            for (i, val) in vals.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                encode_value(f, val, false)?;
            }
            write!(f, ")")?;
            Ok(())
        }
    }
}

pub fn display_decimal_128(num: i128, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{}", num).unwrap();
    } else {
        let pow_scale = 10_i128.pow(scale as u32);
        if num >= 0 {
            write!(
                buf,
                "{}.{:0>width$}",
                num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        } else {
            write!(
                buf,
                "-{}.{:0>width$}",
                -num / pow_scale,
                (num % pow_scale).abs(),
                width = scale as usize
            )
            .unwrap();
        }
    }
    buf
}

pub fn display_decimal_256(num: i256, scale: u8) -> String {
    let mut buf = String::new();
    if scale == 0 {
        write!(buf, "{}", num).unwrap();
    } else {
        let pow_scale = i256::from_i128(10i128).pow_wrapping(scale as u32);
        let width = scale as usize;
        // -1/10 = 0
        let (int_part, neg) = if num >= i256::ZERO {
            (num / pow_scale, "")
        } else {
            (-num / pow_scale, "-")
        };
        let frac_part = (num % pow_scale).wrapping_abs();

        match frac_part.to_i128() {
            Some(frac_part) => {
                write!(
                    buf,
                    "{}{}.{:0>width$}",
                    neg,
                    int_part,
                    frac_part,
                    width = width
                )
                .unwrap();
            }
            None => {
                // fractional part is too big for display,
                // split it into two parts.
                let pow = i256::from_i128(10i128).pow_wrapping(38);
                let frac_high_part = frac_part / pow;
                let frac_low_part = frac_part % pow;
                let frac_width = (scale - 38) as usize;

                write!(
                    buf,
                    "{}{}.{:0>width$}{}",
                    neg,
                    int_part,
                    frac_high_part.to_i128().unwrap(),
                    frac_low_part.to_i128().unwrap(),
                    width = frac_width
                )
                .unwrap();
            }
        }
    }
    buf
}

/// assume text is from
/// used only for expr, so put more weight on readability
pub fn parse_decimal(text: &str, size: DecimalSize) -> Result<NumberValue> {
    let mut start = 0;
    let bytes = text.as_bytes();
    while start < text.len() && bytes[start] == b'0' {
        start += 1
    }
    let text = &text[start..];
    let point_pos = text.find('.');
    let e_pos = text.find(|c| c == 'e' || c == 'E');
    let (i_part, f_part, e_part) = match (point_pos, e_pos) {
        (Some(p1), Some(p2)) => (&text[..p1], &text[(p1 + 1)..p2], Some(&text[(p2 + 1)..])),
        (Some(p), None) => (&text[..p], &text[(p + 1)..], None),
        (None, Some(p)) => (&text[..p], "", Some(&text[(p + 1)..])),
        (None, None) => (text, "", None),
    };
    let exp = match e_part {
        Some(s) => s.parse::<i32>()?,
        None => 0,
    };
    if i_part.len() as i32 + exp > 76 {
        Err(ConvertError::new("decimal", format!("{:?}", text)).into())
    } else {
        let mut digits = Vec::with_capacity(76);
        digits.extend_from_slice(i_part.as_bytes());
        digits.extend_from_slice(f_part.as_bytes());
        if digits.is_empty() {
            digits.push(b'0')
        }
        let scale = f_part.len() as i32 - exp;
        if scale < 0 {
            // e.g 123.1e3
            for _ in 0..(-scale) {
                digits.push(b'0')
            }
        };

        let precision = std::cmp::min(digits.len(), 76);
        let digits = unsafe { std::str::from_utf8_unchecked(&digits[..precision]) };

        if size.precision > 38 {
            Ok(NumberValue::Decimal256(
                i256::from_string(digits).unwrap(),
                size,
            ))
        } else {
            Ok(NumberValue::Decimal128(digits.parse::<i128>()?, size))
        }
    }
}

pub fn parse_geometry(raw_data: &[u8]) -> Result<String> {
    let mut data = std::io::Cursor::new(raw_data);
    let wkt = Ewkt::from_wkb(&mut data, WkbDialect::Ewkb);
    wkt.map(|g| g.0).map_err(|e| e.into())
}
