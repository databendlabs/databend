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

// Borrow from apache/arrow/rust/datafusion/src/functions.rs
// See notice.md

use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_macros::MallocSizeOf;

use crate::prelude::*;

/// A specific value of a data type.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, MallocSizeOf)]
pub enum DataValue {
    /// Base type.
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(Vec<u8>),

    // Container struct.
    List(Vec<DataValue>),
    Struct(Vec<DataValue>),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, MallocSizeOf)]
pub enum ValueType {
    Null,
    Boolean,
    UInt64,
    Int64,
    Float64,
    String,
    List,
    Struct,
}

pub type DataValueRef = Arc<DataValue>;

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            DataValue::Null => ValueType::Null,
            DataValue::Boolean(_) => ValueType::Boolean,
            DataValue::Int64(_) => ValueType::Int64,
            DataValue::UInt64(_) => ValueType::UInt64,
            DataValue::Float64(_) => ValueType::Float64,
            DataValue::String(_) => ValueType::String,
            DataValue::List(_) => ValueType::List,
            DataValue::Struct(_) => ValueType::Struct,
        }
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(self, |DataValue::Int64(_)| DataValue::UInt64(_))
    }

    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        matches!(self, DataValue::Int64(_))
    }

    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
                | DataValue::UInt64(_)
        )
    }

    pub fn to_series_with_size(&self, size: usize) -> Result<Series> {
        todo!()
        // match self {
        //     DataValue::Null => {
        //         let array = NullArray::new_null(ArrowType::Null, size);
        //         let array: DFNullArray = array.into();
        //         Ok(array.into_series())
        //     }
        //     DataValue::Boolean(values) => Ok(build_constant_series! {DFBooleanArray, values, size}),
        //     DataValue::Int8(values) => Ok(build_constant_series! {DFInt8Array, values, size}),
        //     DataValue::Int16(values) => Ok(build_constant_series! {DFInt16Array, values, size}),
        //     DataValue::Int32(values) => Ok(build_constant_series! {DFInt32Array, values, size}),
        //     DataValue::Int64(values) => Ok(build_constant_series! {DFInt64Array, values, size}),
        //     DataValue::UInt8(values) => Ok(build_constant_series! {DFUInt8Array, values, size}),
        //     DataValue::UInt16(values) => Ok(build_constant_series! {DFUInt16Array, values, size}),
        //     DataValue::UInt32(values) => Ok(build_constant_series! {DFUInt32Array, values, size}),
        //     DataValue::UInt64(values) => Ok(build_constant_series! {DFUInt64Array, values, size}),
        //     DataValue::Float32(values) => Ok(build_constant_series! {DFFloat32Array, values, size}),
        //     DataValue::Float64(values) => Ok(build_constant_series! {DFFloat64Array, values, size}),
        //     DataValue::String(values) => match values {
        //         None => Ok(DFStringArray::full_null(size).into_series()),
        //         v => Ok(DFStringArray::full(v.deref(), size).into_series()),
        //     },
        //     DataValue::List(values) => match data_type {
        //         DataType::Int8 => build_list_series! {i8, values, size, data_type },
        //         DataType::Int16 => build_list_series! {i16, values, size, data_type },
        //         DataType::Int32 => build_list_series! {i32, values, size, data_type },
        //         DataType::Int64 => build_list_series! {i64, values, size, data_type },

        //         DataType::UInt8 => build_list_series! {u8, values, size, data_type },
        //         DataType::UInt16 => build_list_series! {u16, values, size, data_type },
        //         DataType::UInt32 => build_list_series! {u32, values, size, data_type },
        //         DataType::UInt64 => build_list_series! {u64, values, size, data_type },

        //         DataType::Float32 => build_list_series! {f32, values, size, data_type },
        //         DataType::Float64 => build_list_series! {f64, values, size, data_type },

        //         DataType::Boolean => {
        //             let mut builder = ListBooleanArrayBuilder::with_capacity(0, size);
        //             match values {
        //                 v => {
        //                     let series = DataValue::try_into_data_array(v, data_type)?;
        //                     (0..size).for_each(|_| {
        //                         builder.append_series(&series);
        //                     });
        //                 }
        //                 None => (0..size).for_each(|_| {
        //                     builder.append_null();
        //                 }),
        //             }
        //             Ok(builder.finish().into_series())
        //         }
        //         DataType::String => {
        //             let mut builder = ListStringArrayBuilder::with_capacity(0, size);
        //             match values {
        //                 v => {
        //                     let series = DataValue::try_into_data_array(v, data_type)?;
        //                     (0..size).for_each(|_| {
        //                         builder.append_series(&series);
        //                     });
        //                 }
        //                 None => (0..size).for_each(|_| {
        //                     builder.append_null();
        //                 }),
        //             }
        //             Ok(builder.finish().into_series())
        //         }
        //         other => Result::Err(ErrorCode::BadDataValueType(format!(
        //             "Unexpected type:{} for DataValue List",
        //             other
        //         ))),
        //     },
        //     DataValue::Struct(v) => {
        //         let mut arrays = vec![];
        //         let mut fields = vec![];
        //         for (i, x) in v.iter().enumerate() {
        //             let xseries = x.to_series_with_size(size)?;
        //             let val_array = xseries.get_array_ref();

        //             fields.push(ArrowField::new(
        //                 format!("item_{}", i).as_str(),
        //                 val_array.data_type().clone(),
        //                 false,
        //             ));

        //             arrays.push(val_array);
        //         }
        //         let r: DFStructArray =
        //             StructArray::from_data(ArrowType::Struct(fields), arrays, None).into();
        //         Ok(r.into_series())
        //     }
        // }
    }

    pub fn to_values(&self, size: usize) -> Result<Vec<DataValue>> {
        Ok((0..size).map(|_| self.clone()).collect())
    }

    pub fn as_u64(&self) -> Result<u64> {
        match self {
            DataValue::Int64(v) if *v >= 0 => Ok(*v as u64),
            DataValue::UInt64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get u64 number",
                other.value_type()
            ))),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            DataValue::Int64(v) => Ok(*v),
            DataValue::UInt64(v) => Ok(*v as i64),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get i64 number",
                other.value_type()
            ))),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            DataValue::Boolean(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get boolean",
                other.value_type()
            ))),
        }
    }

    pub fn as_f64(&self) -> Result<f64> {
        match self {
            DataValue::Int64(v) => Ok(*v as f64),
            DataValue::UInt64(v) => Ok(*v as f64),
            DataValue::Float64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get f64 number",
                other.value_type()
            ))),
        }
    }

    pub fn as_string(&self) -> Result<Vec<u8>> {
        match self {
            DataValue::Int64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::UInt64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::Float64(v) => Ok(Vec::<u8>::from((*v).to_string())),
            DataValue::String(v) => Ok(v.to_owned()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get string",
                other.value_type()
            ))),
        }
    }
}

// Did not use std::convert:TryFrom
// Because we do not need custom type error.
pub trait DFTryFrom<T>: Sized {
    fn try_from(value: T) -> Result<Self>;
}

impl DFTryFrom<DataValue> for Vec<u8> {
    fn try_from(value: DataValue) -> Result<Self> {
        match value {
            DataValue::String(value) => Ok(value),
            _ => Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error:  Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

std_to_data_value!(Int64, i8);
std_to_data_value!(Int64, i16);
std_to_data_value!(Int64, i32);
std_to_data_value!(Int64, i64);
std_to_data_value!(UInt64, u8);
std_to_data_value!(UInt64, u16);
std_to_data_value!(UInt64, u32);
std_to_data_value!(UInt64, u64);
std_to_data_value!(Float64, f32);
std_to_data_value!(Float64, f64);
std_to_data_value!(Boolean, bool);

impl From<&[u8]> for DataValue {
    fn from(x: &[u8]) -> Self {
        DataValue::String(x.to_vec())
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
        DataValue::String(x)
    }
}

impl From<Option<Vec<u8>>> for DataValue {
    fn from(x: Option<Vec<u8>>) -> Self {
        match x {
            Some(v) => DataValue::String(v),
            None => DataValue::Null,
        }
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_null() {
            return write!(f, "NULL");
        }
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Boolean(v) => write!(f, "{}", v),
            DataValue::Float64(v) => write!(f, "{}", v),
            DataValue::Int64(v) => write!(f, "{}", v),
            DataValue::UInt64(v) => write!(f, "{}", v),
            DataValue::String(v) => match std::str::from_utf8(v) {
                Ok(v) => write!(f, "{}", v),
                Err(_e) => {
                    for c in v {
                        write!(f, "{:02x}", c)?;
                    }
                    Ok(())
                }
            },
            DataValue::List(v, ..) => {
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
            DataValue::Boolean(v) => write!(f, "{}", v),
            DataValue::Int64(v) => write!(f, "{}", v),
            DataValue::UInt64(v) => write!(f, "{}", v),
            DataValue::Float64(v) => write!(f, "{}", v),
            DataValue::String(_) => write!(f, "{}", self),
            DataValue::List(_) => write!(f, "[{}]", self),
            DataValue::Struct(v) => write!(f, "{:?}", v),
        }
    }
}
