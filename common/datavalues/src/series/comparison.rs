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
//! Comparison operations on Series.

use common_exception::ErrorCode;
use common_exception::Result;

use super::Series;
use crate::arrays::ArrayCompare;
use crate::prelude::*;

macro_rules! impl_compare {
    ($self:expr, $rhs:expr, $method:ident) => {{
        if $self.data_type() != $rhs.data_type() {
            return Err(ErrorCode::IllegalDataType(format!(
                "datatype must be some for comparisons, got {} and {}",
                $self.data_type(),
                $rhs.data_type()
            )));
        }

        match $self.data_type() {
            DataType::Boolean => $self.bool().unwrap().$method($rhs.bool().unwrap()),
            DataType::String => $self.string().unwrap().$method($rhs.string().unwrap()),
            DataType::UInt8 => $self.u8().unwrap().$method($rhs.u8().unwrap()),
            DataType::UInt16 => $self.u16().unwrap().$method($rhs.u16().unwrap()),
            DataType::UInt32 => $self.u32().unwrap().$method($rhs.u32().unwrap()),
            DataType::UInt64 => $self.u64().unwrap().$method($rhs.u64().unwrap()),
            DataType::Int8 => $self.i8().unwrap().$method($rhs.i8().unwrap()),
            DataType::Int16 => $self.i16().unwrap().$method($rhs.i16().unwrap()),
            DataType::Int32 => $self.i32().unwrap().$method($rhs.i32().unwrap()),
            DataType::Int64 => $self.i64().unwrap().$method($rhs.i64().unwrap()),
            DataType::Float32 => $self.f32().unwrap().$method($rhs.f32().unwrap()),
            DataType::Float64 => $self.f64().unwrap().$method($rhs.f64().unwrap()),
            DataType::Date16 => $self.u16().unwrap().$method($rhs.u16().unwrap()),
            DataType::Date32 => $self.i32().unwrap().$method($rhs.i32().unwrap()),
            _ => unimplemented!(),
        }
    }};
}

impl ArrayCompare<&Series> for Series {
    /// Create a boolean mask by checking for equality.
    fn eq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), eq)
    }

    /// Create a boolean mask by checking for inequality.
    fn neq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), neq)
    }

    /// Create a boolean mask by checking if lhs > rhs.
    fn gt(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), gt)
    }

    /// Create a boolean mask by checking if lhs >= rhs.
    fn gt_eq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), gt_eq)
    }

    /// Create a boolean mask by checking if lhs < rhs.
    fn lt(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), lt)
    }

    /// Create a boolean mask by checking if lhs <= rhs.
    fn lt_eq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), lt_eq)
    }

    /// Create a boolean mask by checking if lhs < rhs.
    fn like(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), like)
    }

    /// Create a boolean mask by checking if lhs <= rhs.
    fn nlike(&self, rhs: &Series) -> Result<DFBooleanArray> {
        impl_compare!(self.as_ref(), rhs.as_ref(), nlike)
    }
}
