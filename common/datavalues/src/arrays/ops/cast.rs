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
    fn cast_with_type(&self, _data_type: &DataTypePtr) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported cast_with_type operation for {:?}",
            self,
        )))
    }
}

fn cast_ca(ca: &dyn Array, data_type: &DataTypePtr) -> Result<Series> {
    // we did not use logical datatype of arrow, so all arrow datatypes are physical
    let arrow_type = data_type.arrow_type();
    // we enable ignore_overflow by default
    let array = cast::cast(ca, &arrow_type, CastOptions {
        wrapped: true,
        partial: true,
    })?;
    let array: ArrayRef = Arc::from(array);
    Ok(array.into_series())
}

impl<T> ArrayCast for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn cast_with_type(&self, data_type: &DataTypePtr) -> Result<Series> {
        cast_ca(&self.array, data_type)
    }
}

impl ArrayCast for DFNullArray {}
impl ArrayCast for DFListArray {}
impl ArrayCast for DFStructArray {}
