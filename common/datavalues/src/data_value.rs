// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Error;
use anyhow::Result;
use common_arrow::arrow::array::*;
use serde::Deserialize;
use serde::Serialize;

use crate::data_array::BinaryArray;
use crate::BooleanArray;
use crate::DataArrayRef;
use crate::DataField;
use crate::DataType;
use crate::Float32Array;
use crate::Float64Array;
use crate::Int16Array;
use crate::Int32Array;
use crate::Int64Array;
use crate::Int8Array;
use crate::NullArray;
use crate::StringArray;
use crate::StructArray;
use crate::UInt16Array;
use crate::UInt32Array;
use crate::UInt64Array;
use crate::UInt8Array;

/// A specific value of a data type.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum DataValue {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Binary(Option<Vec<u8>>),
    List(Option<Vec<DataValue>>, DataType),
    Utf8(Option<String>),
    Struct(Vec<DataValue>),
}

pub type DataValueRef = Box<DataValue>;

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            DataValue::Boolean(None)
                | DataValue::Int8(None)
                | DataValue::Int16(None)
                | DataValue::Int32(None)
                | DataValue::Int64(None)
                | DataValue::UInt8(None)
                | DataValue::UInt16(None)
                | DataValue::UInt32(None)
                | DataValue::UInt64(None)
                | DataValue::Float32(None)
                | DataValue::Float64(None)
                | DataValue::Utf8(None)
                | DataValue::Binary(None)
                | DataValue::List(None, _)
        )
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DataValue::Null => DataType::Null,
            DataValue::Boolean(_) => DataType::Boolean,
            DataValue::Int8(_) => DataType::Int8,
            DataValue::Int16(_) => DataType::Int16,
            DataValue::Int32(_) => DataType::Int32,
            DataValue::Int64(_) => DataType::Int64,
            DataValue::UInt8(_) => DataType::UInt8,
            DataValue::UInt16(_) => DataType::UInt16,
            DataValue::UInt32(_) => DataType::UInt32,
            DataValue::UInt64(_) => DataType::UInt64,
            DataValue::Float32(_) => DataType::Float32,
            DataValue::Float64(_) => DataType::Float64,
            DataValue::Utf8(_) => DataType::Utf8,
            DataValue::Binary(_) => DataType::Binary,
            DataValue::List(_, data_type) => {
                DataType::List(Box::new(DataField::new("item", data_type.clone(), true)))
            }
            DataValue::Struct(v) => {
                let fields = v
                    .iter()
                    .enumerate()
                    .map(|(i, x)| {
                        let typ = x.data_type();
                        DataField::new(format!("item_{}", i).as_str(), typ, true)
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(fields)
            }
        }
    }

    // LiteralValue from planner, we turn Int64, UInt64 to specific minimal type
    pub fn to_field_value(&self) -> Self {
        match *self {
            DataValue::Int64(Some(i)) => {
                if i < i8::MAX as i64 {
                    return DataValue::Int8(Some(i as i8));
                }
                if i < i16::MAX as i64 {
                    return DataValue::Int16(Some(i as i16));
                }
                if i < i32::MAX as i64 {
                    return DataValue::Int32(Some(i as i32));
                }
                self.clone()
            }

            DataValue::UInt64(Some(i)) => {
                if i < u8::MAX as u64 {
                    return DataValue::UInt8(Some(i as u8));
                }
                if i < u16::MAX as u64 {
                    return DataValue::UInt16(Some(i as u16));
                }
                if i < u32::MAX as u64 {
                    return DataValue::UInt32(Some(i as u32));
                }
                self.clone()
            }
            _ => self.clone(),
        }
    }

    pub fn to_array_with_size(&self, size: usize) -> Result<DataArrayRef> {
        Ok(match self {
            DataValue::Null => Arc::new(NullArray::new(size)),
            DataValue::Boolean(Some(v)) => {
                Arc::new(BooleanArray::from(vec![*v; size])) as DataArrayRef
            }
            DataValue::Int8(Some(v)) => Arc::new(Int8Array::from(vec![*v; size])) as DataArrayRef,
            DataValue::Int16(Some(v)) => Arc::new(Int16Array::from(vec![*v; size])) as DataArrayRef,
            DataValue::Int32(Some(v)) => Arc::new(Int32Array::from(vec![*v; size])) as DataArrayRef,
            DataValue::Int64(Some(v)) => Arc::new(Int64Array::from(vec![*v; size])) as DataArrayRef,
            DataValue::UInt8(Some(v)) => Arc::new(UInt8Array::from(vec![*v; size])) as DataArrayRef,
            DataValue::UInt16(Some(v)) => {
                Arc::new(UInt16Array::from(vec![*v; size])) as DataArrayRef
            }
            DataValue::UInt32(Some(v)) => {
                Arc::new(UInt32Array::from(vec![*v; size])) as DataArrayRef
            }
            DataValue::UInt64(Some(v)) => {
                Arc::new(UInt64Array::from(vec![*v; size])) as DataArrayRef
            }
            DataValue::Float32(Some(v)) => {
                Arc::new(Float32Array::from(vec![*v; size])) as DataArrayRef
            }
            DataValue::Float64(Some(v)) => {
                Arc::new(Float64Array::from(vec![*v; size])) as DataArrayRef
            }
            DataValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_deref(); size])),
            DataValue::Binary(v) => Arc::new(BinaryArray::from(vec![v.as_deref(); size])),
            DataValue::List(values, data_type) => Arc::new(match data_type {
                DataType::Int8 => build_list!(Int8Builder, Int8, values, size),
                DataType::Int16 => build_list!(Int16Builder, Int16, values, size),
                DataType::Int32 => build_list!(Int32Builder, Int32, values, size),
                DataType::Int64 => build_list!(Int64Builder, Int64, values, size),
                DataType::UInt8 => build_list!(UInt8Builder, UInt8, values, size),
                DataType::UInt16 => build_list!(UInt16Builder, UInt16, values, size),
                DataType::UInt32 => build_list!(UInt32Builder, UInt32, values, size),
                DataType::UInt64 => build_list!(UInt64Builder, UInt64, values, size),
                DataType::Utf8 => build_list!(StringBuilder, Utf8, values, size),
                other => bail!("Unexpected DataType:{} for list", other),
            }),
            DataValue::Struct(v) => {
                let mut array = vec![];
                for (i, x) in v.iter().enumerate() {
                    let val_array = x.to_array_with_size(1)?;
                    array.push((
                        DataField::new(
                            format!("item_{}", i).as_str(),
                            val_array.data_type().clone(),
                            false,
                        ),
                        val_array as DataArrayRef,
                    ));
                }
                Arc::new(StructArray::from(array))
            }
            other => {
                bail!(format!(
                    "DataValue Error: DataValue to array cannot be {:?}",
                    other
                ));
            }
        })
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &DataArrayRef, index: usize) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Boolean => {
                typed_cast_from_array_to_data_value!(array, index, BooleanArray, Boolean)
            }
            DataType::Float64 => {
                typed_cast_from_array_to_data_value!(array, index, Float64Array, Float64)
            }
            DataType::Float32 => {
                typed_cast_from_array_to_data_value!(array, index, Float32Array, Float32)
            }
            DataType::UInt64 => {
                typed_cast_from_array_to_data_value!(array, index, UInt64Array, UInt64)
            }
            DataType::UInt32 => {
                typed_cast_from_array_to_data_value!(array, index, UInt32Array, UInt32)
            }
            DataType::UInt16 => {
                typed_cast_from_array_to_data_value!(array, index, UInt16Array, UInt16)
            }
            DataType::UInt8 => {
                typed_cast_from_array_to_data_value!(array, index, UInt8Array, UInt8)
            }
            DataType::Int64 => {
                typed_cast_from_array_to_data_value!(array, index, Int64Array, Int64)
            }
            DataType::Int32 => {
                typed_cast_from_array_to_data_value!(array, index, Int32Array, Int32)
            }
            DataType::Int16 => {
                typed_cast_from_array_to_data_value!(array, index, Int16Array, Int16)
            }
            DataType::Int8 => typed_cast_from_array_to_data_value!(array, index, Int8Array, Int8),
            DataType::Utf8 => {
                typed_cast_from_array_to_data_value!(array, index, StringArray, Utf8)
            }
            DataType::Binary => {
                typed_cast_from_array_to_data_value!(array, index, BinaryArray, Binary)
            }
            DataType::List(nested_type) => {
                let list_array = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast ListArray"))?;
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let scalar_vec = (0..nested_array.len())
                            .map(|i| DataValue::try_from_array(&nested_array, i))
                            .collect::<Result<Vec<_>>>()?;
                        Some(scalar_vec)
                    }
                };
                DataValue::List(value, nested_type.data_type().clone())
            }
            DataType::Struct(_) => {
                let strut_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast StructArray".to_string()))?;
                let nested_array = strut_array.column(index);
                let scalar_vec = (0..nested_array.len())
                    .map(|i| DataValue::try_from_array(&nested_array, i))
                    .collect::<Result<Vec<_>>>()?;
                DataValue::Struct(scalar_vec)
            }
            other => {
                bail!(format!(
                    "DataValue Error: Can't create a scalar of array of type \"{:?}\"",
                    other
                ));
            }
        })
    }

    pub fn try_from_literal(literal: &str) -> Result<Self> {
        match literal.parse::<i64>() {
            Ok(n) => {
                if n >= 0 {
                    Ok(DataValue::UInt64(Some(n as u64)))
                } else {
                    Ok(DataValue::Int64(Some(n)))
                }
            }
            Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?))),
        }
    }
}

typed_cast_from_data_value_to_std!(Int8, i8);
typed_cast_from_data_value_to_std!(Int16, i16);
typed_cast_from_data_value_to_std!(Int32, i32);
typed_cast_from_data_value_to_std!(Int64, i64);
typed_cast_from_data_value_to_std!(UInt8, u8);
typed_cast_from_data_value_to_std!(UInt16, u16);
typed_cast_from_data_value_to_std!(UInt32, u32);
typed_cast_from_data_value_to_std!(UInt64, u64);
typed_cast_from_data_value_to_std!(Float32, f32);
typed_cast_from_data_value_to_std!(Float64, f64);
typed_cast_from_data_value_to_std!(Boolean, bool);

impl TryFrom<&DataType> for DataValue {
    type Error = Error;

    fn try_from(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Null => DataValue::Null,
            DataType::Boolean => DataValue::Boolean(None),
            DataType::Int8 => DataValue::Int8(None),
            DataType::Int16 => DataValue::Int16(None),
            DataType::Int32 => DataValue::Int32(None),
            DataType::Int64 => DataValue::Int64(None),
            DataType::UInt8 => DataValue::UInt8(None),
            DataType::UInt16 => DataValue::UInt16(None),
            DataType::UInt32 => DataValue::UInt32(None),
            DataType::UInt64 => DataValue::UInt64(None),
            DataType::Float32 => DataValue::Float32(None),
            DataType::Float64 => DataValue::Float64(None),
            _ => {
                bail!(format!(
                    "DataValue Error: Unsupported try_from() for data type: {:?}",
                    data_type
                ));
            }
        })
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "Null"),
            DataValue::Boolean(v) => format_data_value_with_option!(f, v),
            DataValue::Float32(v) => format_data_value_with_option!(f, v),
            DataValue::Float64(v) => format_data_value_with_option!(f, v),
            DataValue::Int8(v) => format_data_value_with_option!(f, v),
            DataValue::Int16(v) => format_data_value_with_option!(f, v),
            DataValue::Int32(v) => format_data_value_with_option!(f, v),
            DataValue::Int64(v) => format_data_value_with_option!(f, v),
            DataValue::UInt8(v) => format_data_value_with_option!(f, v),
            DataValue::UInt16(v) => format_data_value_with_option!(f, v),
            DataValue::UInt32(v) => format_data_value_with_option!(f, v),
            DataValue::UInt64(v) => format_data_value_with_option!(f, v),
            DataValue::Utf8(v) => format_data_value_with_option!(f, v),
            DataValue::Binary(None) => write!(f, "NULL"),
            DataValue::Binary(Some(v)) => write!(
                f,
                "{}",
                v.iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            DataValue::List(None, ..) => write!(f, "NULL"),
            DataValue::List(Some(v), ..) => {
                write!(
                    f,
                    "{}",
                    v.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "Null"),
            DataValue::Boolean(v) => format_data_value_with_option!(f, v),
            DataValue::Int8(v) => format_data_value_with_option!(f, v),
            DataValue::Int16(v) => format_data_value_with_option!(f, v),
            DataValue::Int32(v) => format_data_value_with_option!(f, v),
            DataValue::Int64(v) => format_data_value_with_option!(f, v),
            DataValue::UInt8(v) => format_data_value_with_option!(f, v),
            DataValue::UInt16(v) => format_data_value_with_option!(f, v),
            DataValue::UInt32(v) => format_data_value_with_option!(f, v),
            DataValue::UInt64(v) => format_data_value_with_option!(f, v),
            DataValue::Float32(v) => format_data_value_with_option!(f, v),
            DataValue::Float64(v) => format_data_value_with_option!(f, v),
            DataValue::Utf8(v) => format_data_value_with_option!(f, v),
            DataValue::Binary(None) => write!(f, "{}", self),
            DataValue::Binary(Some(_)) => write!(f, "\"{}\"", self),
            DataValue::List(_, _) => write!(f, "[{}]", self),
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}
