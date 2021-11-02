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

// Borrow from apache/arrow/rust/datafusion/src/functions.rs
// See notice.md

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use common_macros::MallocSizeOf;

use crate::arrays::ListBooleanArrayBuilder;
use crate::arrays::ListBuilderTrait;
use crate::arrays::ListPrimitiveArrayBuilder;
use crate::arrays::ListStringArrayBuilder;
use crate::prelude::*;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DataField;

/// A specific value of a data type.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, MallocSizeOf)]
pub enum DataValue {
    /// Base type.
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
    String(Option<Vec<u8>>),

    // Container struct.
    List(Option<Vec<DataValue>>, DataType),
    Struct(Vec<DataValue>),
}

pub type DataValueRef = Arc<DataValue>;

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
                | DataValue::String(None)
                | DataValue::Null
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
            DataValue::String(_) => DataType::String,
        }
    }

    pub fn to_array(&self) -> Result<Series> {
        self.to_series_with_size(1)
    }

    pub fn to_series_with_size(&self, size: usize) -> Result<Series> {
        match self {
            DataValue::Null => {
                let array = NullArray::new_null(ArrowType::Null, size);
                let array: DFNullArray = array.into();
                Ok(array.into_series())
            }
            DataValue::Boolean(values) => Ok(build_constant_series! {DFBooleanArray, values, size}),
            DataValue::Int8(values) => Ok(build_constant_series! {DFInt8Array, values, size}),
            DataValue::Int16(values) => Ok(build_constant_series! {DFInt16Array, values, size}),
            DataValue::Int32(values) => Ok(build_constant_series! {DFInt32Array, values, size}),
            DataValue::Int64(values) => Ok(build_constant_series! {DFInt64Array, values, size}),
            DataValue::UInt8(values) => Ok(build_constant_series! {DFUInt8Array, values, size}),
            DataValue::UInt16(values) => Ok(build_constant_series! {DFUInt16Array, values, size}),
            DataValue::UInt32(values) => Ok(build_constant_series! {DFUInt32Array, values, size}),
            DataValue::UInt64(values) => Ok(build_constant_series! {DFUInt64Array, values, size}),
            DataValue::Float32(values) => Ok(build_constant_series! {DFFloat32Array, values, size}),
            DataValue::Float64(values) => Ok(build_constant_series! {DFFloat64Array, values, size}),
            DataValue::String(values) => match values {
                None => Ok(DFStringArray::full_null(size).into_series()),
                Some(v) => Ok(DFStringArray::full(v.deref(), size).into_series()),
            },
            DataValue::List(values, data_type) => match data_type {
                DataType::Int8 => build_list_series! {i8, values, size, data_type },
                DataType::Int16 => build_list_series! {i16, values, size, data_type },
                DataType::Int32 => build_list_series! {i32, values, size, data_type },
                DataType::Int64 => build_list_series! {i64, values, size, data_type },

                DataType::UInt8 => build_list_series! {u8, values, size, data_type },
                DataType::UInt16 => build_list_series! {u16, values, size, data_type },
                DataType::UInt32 => build_list_series! {u32, values, size, data_type },
                DataType::UInt64 => build_list_series! {u64, values, size, data_type },

                DataType::Float32 => build_list_series! {f32, values, size, data_type },
                DataType::Float64 => build_list_series! {f64, values, size, data_type },

                DataType::Boolean => {
                    let mut builder = ListBooleanArrayBuilder::with_capacity(0, size);
                    match values {
                        Some(v) => {
                            let series = DataValue::try_into_data_array(v, data_type)?;
                            (0..size).for_each(|_| {
                                builder.append_series(&series);
                            });
                        }
                        None => (0..size).for_each(|_| {
                            builder.append_null();
                        }),
                    }
                    Ok(builder.finish().into_series())
                }
                DataType::String => {
                    let mut builder = ListStringArrayBuilder::with_capacity(0, size);
                    match values {
                        Some(v) => {
                            let series = DataValue::try_into_data_array(v, data_type)?;
                            (0..size).for_each(|_| {
                                builder.append_series(&series);
                            });
                        }
                        None => (0..size).for_each(|_| {
                            builder.append_null();
                        }),
                    }
                    Ok(builder.finish().into_series())
                }
                other => Result::Err(ErrorCode::BadDataValueType(format!(
                    "Unexpected type:{} for DataValue List",
                    other
                ))),
            },
            DataValue::Struct(v) => {
                let mut arrays = vec![];
                let mut fields = vec![];
                for (i, x) in v.iter().enumerate() {
                    let xseries = x.to_series_with_size(size)?;
                    let val_array = xseries.get_array_ref();

                    fields.push(ArrowField::new(
                        format!("item_{}", i).as_str(),
                        val_array.data_type().clone(),
                        false,
                    ));

                    arrays.push(val_array);
                }
                let r: DFStructArray =
                    StructArray::from_data(ArrowType::Struct(fields), arrays, None).into();
                Ok(r.into_series())
            }
        }
    }

    pub fn to_values(&self, size: usize) -> Result<Vec<DataValue>> {
        Ok((0..size).map(|_| self.clone()).collect())
    }

    pub fn as_u64(&self) -> Result<u64> {
        match self {
            DataValue::Int8(Some(v)) => Ok(*v as u64),
            DataValue::Int16(Some(v)) => Ok(*v as u64),
            DataValue::Int32(Some(v)) => Ok(*v as u64),
            DataValue::Int64(Some(v)) => Ok(*v as u64),
            DataValue::UInt8(Some(v)) => Ok(*v as u64),
            DataValue::UInt16(Some(v)) => Ok(*v as u64),
            DataValue::UInt32(Some(v)) => Ok(*v as u64),
            DataValue::UInt64(Some(v)) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get u64 number",
                other.data_type()
            ))),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            DataValue::Int8(Some(v)) => Ok(*v as i64),
            DataValue::Int16(Some(v)) => Ok(*v as i64),
            DataValue::Int32(Some(v)) => Ok(*v as i64),
            DataValue::Int64(Some(v)) => Ok(*v),
            DataValue::UInt8(Some(v)) => Ok(*v as i64),
            DataValue::UInt16(Some(v)) => Ok(*v as i64),
            DataValue::UInt32(Some(v)) => Ok(*v as i64),
            DataValue::UInt64(Some(v)) => Ok(*v as i64),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get i64 number",
                other.data_type()
            ))),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            DataValue::Null => Ok(false),
            DataValue::Boolean(v) => Ok(v.map_or(false, |v| v)),
            DataValue::Int8(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::Int16(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::Int32(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::Int64(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::UInt8(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::UInt16(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::UInt32(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::UInt64(v) => Ok(v.map_or(false, |v| v != 0)),
            DataValue::Float32(v) => Ok(v.map_or(false, |v| v != 0f32)),
            DataValue::Float64(v) => Ok(v.map_or(false, |v| v != 0f64)),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get boolean",
                other.data_type()
            ))),
        }
    }
}

// Did not use std::convert:TryFrom
// Because we do not need custom type error.
pub trait DFTryFrom<T>: Sized {
    fn try_from(value: T) -> Result<Self>;
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

impl DFTryFrom<DataValue> for Vec<u8> {
    fn try_from(value: DataValue) -> Result<Self> {
        match value {
            DataValue::String(Some(inner_value)) => Ok(inner_value),
            _ => Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error:  Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

std_to_data_value!(Int8, i8);
std_to_data_value!(Int16, i16);
std_to_data_value!(Int32, i32);
std_to_data_value!(Int64, i64);
std_to_data_value!(UInt8, u8);
std_to_data_value!(UInt16, u16);
std_to_data_value!(UInt32, u32);
std_to_data_value!(UInt64, u64);
std_to_data_value!(Float32, f32);
std_to_data_value!(Float64, f64);
std_to_data_value!(Boolean, bool);

impl From<&[u8]> for DataValue {
    fn from(x: &[u8]) -> Self {
        DataValue::String(Some(x.to_vec()))
    }
}

impl From<Option<&[u8]>> for DataValue {
    fn from(x: Option<&[u8]>) -> Self {
        let x = x.map(|c| c.to_vec());
        DataValue::from(x)
    }
}

impl From<Vec<u8>> for DataValue {
    fn from(x: Vec<u8>) -> Self {
        DataValue::String(Some(x))
    }
}

impl From<Option<Vec<u8>>> for DataValue {
    fn from(x: Option<Vec<u8>>) -> Self {
        DataValue::String(x)
    }
}

impl From<&DataType> for DataValue {
    fn from(data_type: &DataType) -> Self {
        match data_type {
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
            DataType::Date16 => DataValue::UInt16(None),
            DataType::Date32 => DataValue::Int32(None),
            DataType::DateTime32(_) => DataValue::UInt32(None),
            DataType::List(f) => DataValue::List(None, f.data_type().clone()),
            DataType::Struct(_) => DataValue::Struct(vec![]),
            DataType::String => DataValue::String(None),
            DataType::Interval(_) => DataValue::Int64(None),
        }
    }
}

impl From<DataType> for DataValue {
    fn from(data_type: DataType) -> Self {
        DataValue::from(&data_type)
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_null() {
            return write!(f, "NULL");
        }
        match self {
            DataValue::Null => write!(f, "NULL"),
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
            DataValue::String(None) => write!(f, "NULL"),
            DataValue::String(Some(v)) => match std::str::from_utf8(v) {
                Ok(v) => write!(f, "{}", v),
                Err(_e) => {
                    for c in v {
                        write!(f, "{:02x}", c)?;
                    }
                    Ok(())
                }
            },
            DataValue::List(None, ..) => write!(f, "NULL"),
            DataValue::List(Some(v), ..) => {
                write!(
                    f,
                    "[{}]",
                    v.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            return write!(f, "NULL");
        }
        match self {
            DataValue::Null => write!(f, "NULL"),
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
            DataValue::String(None) => write!(f, "{}", self),
            DataValue::String(Some(_)) => write!(f, "{}", self),
            DataValue::List(_, _) => write!(f, "[{}]", self),
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}

impl BinarySer for DataValue {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<()> {
        let bs = serde_json::to_string(self)?;
        writer.write_string(bs)
    }

    fn serialize_to_buf<W: BufMut>(&self, writer: &mut W) -> Result<()> {
        let bs = serde_json::to_string(self)?;
        writer.write_string(bs)
    }
}

impl BinaryDe for DataValue {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        let str = reader.read_string()?;
        let value: DataValue = serde_json::from_str(&str)?;
        Ok(value)
    }
}
