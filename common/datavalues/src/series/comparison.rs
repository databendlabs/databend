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

use common_exception::Result;

use super::Series;
use crate::arrays::ArrayCompare;
use crate::numerical_coercion;
use crate::prelude::*;

macro_rules! impl_compare {
    ($self:expr, $rhs:expr, $method:ident) => {{
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
            DataType::Date32 => $self.u32().unwrap().$method($rhs.u32().unwrap()),
            _ => unimplemented!(),
        }
    }};
}

fn null_to_boolean(s: &Series) -> DFBooleanArray {
    if s.data_type() == &DataType::Null {
        DFBooleanArray::full_null(s.len())
    } else {
        let array_ref = s.get_array_ref();
        let validity = array_ref.validity();
        match validity {
            Some(v) => DFBooleanArray::new_from_opt_iter(v.into_iter().map(|c| {
                if c {
                    Some(true)
                } else {
                    None
                }
            })),
            None => DFBooleanArray::full(true, s.len()),
        }
    }
}

fn coerce_cmp_lhs_rhs(lhs: &Series, rhs: &Series) -> Result<(Series, Series)> {
    if lhs.data_type() == rhs.data_type()
        && (lhs.data_type() == &DataType::String || lhs.data_type() == &DataType::Boolean)
    {
        return Ok((lhs.clone(), rhs.clone()));
    }

    if lhs.data_type() == &DataType::Null || rhs.data_type() == &DataType::Null {
        let lhs = null_to_boolean(lhs);
        let rhs = null_to_boolean(rhs);

        return Ok((lhs.into_series(), rhs.into_series()));
    }

    let dtype = numerical_coercion(lhs.data_type(), rhs.data_type(), true)?;

    let mut left = lhs.clone();
    if lhs.data_type() != &dtype {
        left = lhs.cast_with_type(&dtype)?;
    }

    let mut right = rhs.clone();
    if rhs.data_type() != &dtype {
        right = rhs.cast_with_type(&dtype)?;
    }

    Ok((left, right))
}

impl ArrayCompare<&Series> for Series {
    /// Create a boolean mask by checking for equality.
    fn eq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), eq)
    }

    /// Create a boolean mask by checking for inequality.
    fn neq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), neq)
    }

    /// Create a boolean mask by checking if lhs > rhs.
    fn gt(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), gt)
    }

    /// Create a boolean mask by checking if lhs >= rhs.
    fn gt_eq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), gt_eq)
    }

    /// Create a boolean mask by checking if lhs < rhs.
    fn lt(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), lt)
    }

    /// Create a boolean mask by checking if lhs <= rhs.
    fn lt_eq(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), lt_eq)
    }

    /// Create a boolean mask by checking if lhs < rhs.
    fn like(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), like)
    }

    /// Create a boolean mask by checking if lhs <= rhs.
    fn nlike(&self, rhs: &Series) -> Result<DFBooleanArray> {
        let (lhs, rhs) = coerce_cmp_lhs_rhs(self, rhs)?;
        impl_compare!(lhs.as_ref(), rhs.as_ref(), nlike)
    }
}
