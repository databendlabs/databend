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
        if matches!(data_type, &DataType::Float32(_)) {
            let c = self.apply_cast_numeric(|v| {
                lexical_core::parse_partial::<f32>(v)
                    .unwrap_or((0.0f32, 0))
                    .0
            });

            Ok(c.into_series())
        } else if matches!(data_type, &DataType::Float64(_)) {
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
    // TODO: only nullable data_type can call full_null
    fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
        match data_type {
            DataType::Null => Ok(self.clone().into_series()),
            DataType::Boolean(_) => Ok(DFBooleanArray::full_null(self.len()).into_series()),
            DataType::UInt8(_) => Ok(DFUInt8Array::full_null(self.len()).into_series()),
            DataType::UInt16(_) => Ok(DFUInt16Array::full_null(self.len()).into_series()),
            DataType::UInt32(_) => Ok(DFUInt32Array::full_null(self.len()).into_series()),
            DataType::UInt64(_) => Ok(DFUInt64Array::full_null(self.len()).into_series()),
            DataType::Int8(_) => Ok(DFInt8Array::full_null(self.len()).into_series()),
            DataType::Int16(_) => Ok(DFInt16Array::full_null(self.len()).into_series()),
            DataType::Int32(_) => Ok(DFInt32Array::full_null(self.len()).into_series()),
            DataType::Int64(_) => Ok(DFInt64Array::full_null(self.len()).into_series()),
            DataType::Float32(_) => Ok(DFFloat32Array::full_null(self.len()).into_series()),
            DataType::Float64(_) => Ok(DFFloat64Array::full_null(self.len()).into_series()),
            DataType::String(_) => Ok(DFStringArray::full_null(self.len()).into_series()),
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
