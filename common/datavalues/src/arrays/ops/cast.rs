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

use std::fmt::Debug;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::cast;
use common_exception::ErrorCode;
use common_exception::Result;
use num::NumCast;

use crate::prelude::*;
use crate::series::IntoSeries;
use crate::series::Series;

/// Cast `DataArray<T>` to `DataArray<N>`
pub trait ArrayCast: Debug {
    /// Cast `DataArray<T>` to `DataArray<N>`
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported cast operation for {:?}",
            self,
        )))
    }

    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported cast_with_type operation for {:?}",
            self,
        )))
    }
}

fn cast_ca<N, T>(ca: &DataArray<T>) -> Result<DataArray<N>>
where
    N: DFDataType,
    T: DFDataType,
{
    if N::data_type() == T::data_type() {
        // convince the compiler that N and T are the same type
        return unsafe {
            let ca = std::mem::transmute(ca.clone());
            Ok(ca)
        };
    }

    // we enable ignore_overflow by default
    let ca: ArrayRef = Arc::from(cast::wrapping_cast(
        ca.array.as_ref(),
        &N::data_type().to_arrow(),
    )?);
    Ok(ca.into())
}

macro_rules! cast_with_type {
    ($self:expr, $data_type:expr) => {{
        match $data_type {
            DataType::Boolean => ArrayCast::cast::<BooleanType>($self).map(|ca| ca.into_series()),
            DataType::Utf8 => ArrayCast::cast::<Utf8Type>($self).map(|ca| ca.into_series()),
            DataType::UInt8 => ArrayCast::cast::<UInt8Type>($self).map(|ca| ca.into_series()),
            DataType::UInt16 => ArrayCast::cast::<UInt16Type>($self).map(|ca| ca.into_series()),
            DataType::UInt32 => ArrayCast::cast::<UInt32Type>($self).map(|ca| ca.into_series()),
            DataType::UInt64 => ArrayCast::cast::<UInt64Type>($self).map(|ca| ca.into_series()),
            DataType::Int8 => ArrayCast::cast::<Int8Type>($self).map(|ca| ca.into_series()),
            DataType::Int16 => ArrayCast::cast::<Int16Type>($self).map(|ca| ca.into_series()),
            DataType::Int32 => ArrayCast::cast::<Int32Type>($self).map(|ca| ca.into_series()),
            DataType::Int64 => ArrayCast::cast::<Int64Type>($self).map(|ca| ca.into_series()),
            DataType::Float32 => ArrayCast::cast::<Float32Type>($self).map(|ca| ca.into_series()),
            DataType::Float64 => ArrayCast::cast::<Float64Type>($self).map(|ca| ca.into_series()),
            DataType::Date32 => ArrayCast::cast::<Date32Type>($self).map(|ca| ca.into_series()),
            DataType::Date64 => ArrayCast::cast::<Date64Type>($self).map(|ca| ca.into_series()),

            DataType::List(_) => ArrayCast::cast::<ListType>($self).map(|ca| ca.into_series()),
            dt => Err(ErrorCode::IllegalDataType(format!(
                "Arrow datatype {:?} not supported by Datafuse",
                dt
            ))),
        }
    }};
}

impl<T> ArrayCast for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
{
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(self)
    }

    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_with_type!(self, data_type)
    }
}

impl ArrayCast for DFUtf8Array {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(self)
    }

    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_with_type!(self, data_type)
    }
}

impl ArrayCast for DFBooleanArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(self)
    }
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_with_type!(self, data_type)
    }
}

impl ArrayCast for DFNullArray {
    fn cast<N>(&self) -> Result<DataArray<N>>
    where N: DFDataType {
        cast_ca(self)
    }

    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        match data_type {
            DataType::Null => Ok(self.clone().into_series()),
            DataType::Boolean => Ok(DFBooleanArray::full_null(self.len()).into_series()),
            DataType::Utf8 => Ok(DFUtf8Array::full_null(self.len()).into_series()),
            DataType::UInt8 => Ok(DFUInt8Array::full_null(self.len()).into_series()),
            DataType::UInt16 => Ok(DFUInt16Array::full_null(self.len()).into_series()),
            DataType::UInt32 => Ok(DFUInt32Array::full_null(self.len()).into_series()),
            DataType::UInt64 => Ok(DFUInt64Array::full_null(self.len()).into_series()),
            DataType::Int8 => Ok(DFInt8Array::full_null(self.len()).into_series()),
            DataType::Int16 => Ok(DFInt16Array::full_null(self.len()).into_series()),
            DataType::Int32 => Ok(DFInt32Array::full_null(self.len()).into_series()),
            DataType::Int64 => Ok(DFInt64Array::full_null(self.len()).into_series()),
            DataType::Float32 => Ok(DFFloat32Array::full_null(self.len()).into_series()),
            DataType::Float64 => Ok(DFFloat64Array::full_null(self.len()).into_series()),
            DataType::Date32 => Ok(DFDate32Array::full_null(self.len()).into_series()),
            DataType::Date64 => Ok(DFDate64Array::full_null(self.len()).into_series()),
            DataType::Binary => Ok(DFBinaryArray::full_null(self.len()).into_series()),
            DataType::List(_) => Ok(DFListArray::full_null(self.len()).into_series()),

            _ => Err(ErrorCode::BadDataValueType(format!(
                "Unsupported cast_with_type operation for {:?}",
                self,
            ))),
        }
    }
}

impl ArrayCast for DFListArray {}
impl ArrayCast for DFBinaryArray {}
impl ArrayCast for DFStructArray {}
