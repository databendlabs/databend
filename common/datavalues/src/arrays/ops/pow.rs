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

//! Implementations of arithmetic operations on DataArray's.

use common_arrow::arrow::compute::arithmetics::basic;

use crate::prelude::*;

pub trait Pow {
    fn pow_f32(&self, _exp: f32) -> DFFloat32Array {
        unimplemented!()
    }
    fn pow_f64(&self, _exp: f64) -> DFFloat64Array {
        unimplemented!()
    }
}

impl<T> Pow for DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    DFPrimitiveArray<T>: ArrayCast,
{
    fn pow_f32(&self, exp: f32) -> DFFloat32Array {
        let arr = self.cast_with_type(&DataType::Float32).expect("f32 array");
        let arr = arr.f32().unwrap();
        DFFloat32Array::new(basic::powf_scalar(&arr.array, exp))
    }

    fn pow_f64(&self, exp: f64) -> DFFloat64Array {
        let arr = self.cast_with_type(&DataType::Float64).expect("f64 array");
        let arr = arr.f64().unwrap();

        DFFloat64Array::new(basic::powf_scalar(&arr.array, exp))
    }
}

impl Pow for DFBooleanArray {}
impl Pow for DFStringArray {}
impl Pow for DFListArray {}
