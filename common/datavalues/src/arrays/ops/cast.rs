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

use std::fmt::Debug;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::cast;
use common_arrow::arrow::compute::cast::CastOptions;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

pub trait ArrayCast: Debug {
    fn cast_with_type(&self, _data_type: &DataType) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported cast_with_type operation for {:?}",
            self,
        )))
    }
}

fn cast_ca(ca: &dyn Array, data_type: &DataType) -> Result<Series> {
    let arrow_type = data_type.to_arrow();
    let arrow_type = get_physical_arrow_type(&arrow_type);
    // we enable ignore_overflow by default
    let array = cast::cast(ca, arrow_type, CastOptions {
        wrapped: true,
        partial: true,
    })?;
    let array: ArrayRef = Arc::from(array);
    Ok(array.into_series())
}

impl<T> ArrayCast for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_ca(&self.array, data_type)
    }
}

impl ArrayCast for DFStringArray {
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        // special case for string to float
        if data_type == &DataType::Float32 {
            let c = self.apply_cast_numeric(|v| {
                lexical_core::parse_partial::<f32>(v)
                    .unwrap_or((0.0f32, 0))
                    .0
            });

            Ok(c.into_series())
        } else if data_type == &DataType::Float64 {
            let c = self.apply_cast_numeric(|v| {
                lexical_core::parse_partial::<f64>(v)
                    .unwrap_or((0.0f64, 0))
                    .0
            });

            Ok(c.into_series())
        } else {
            cast_ca(&self.array, data_type)
        }
    }
}

impl ArrayCast for DFBooleanArray {
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        cast_ca(&self.array, data_type)
    }
}

impl ArrayCast for DFNullArray {
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        match data_type {
            DataType::Null => Ok(self.clone().into_series()),
            DataType::Boolean(_) => Ok(DFBooleanArray::full_null(self.len()).into_series()),
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
            DataType::String => Ok(DFStringArray::full_null(self.len()).into_series()),
            DataType::List(_) => Ok(DFListArray::full_null(self.len()).into_series()),

            _ => Err(ErrorCode::BadDataValueType(format!(
                "Unsupported cast_with_type from array: {:?} into data_type: {:?}",
                self, data_type,
            ))),
        }
    }
}

impl ArrayCast for DFListArray {}
impl ArrayCast for DFStructArray {}
