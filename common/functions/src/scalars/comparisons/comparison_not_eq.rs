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

use common_arrow::arrow::compute::comparison::Simd8;
use common_arrow::arrow::compute::comparison::Simd8PartialEq;
use common_datavalues::prelude::*;
use num::traits::AsPrimitive;

use super::comparison::ComparisonFunctionCreator;
use super::comparison::ComparisonImpl;
use super::utils::*;
use crate::scalars::EvalContext;

pub type ComparisonNotEqFunction = ComparisonFunctionCreator<ComparisonNotEqImpl>;

#[derive(Clone)]
pub struct ComparisonNotEqImpl;

impl ComparisonImpl for ComparisonNotEqImpl {
    type PrimitiveSimd = PrimitiveSimdNotEq;
    type BooleanSimd = BooleanSimdNotEq;

    fn eval_primitive<L, R, M>(l: L::RefType<'_>, r: R::RefType<'_>, _ctx: &mut EvalContext) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType,
    {
        l.to_owned_scalar().as_().ne(&r.to_owned_scalar().as_())
    }

    fn eval_binary(l: &[u8], r: &[u8], _ctx: &mut EvalContext) -> bool {
        l != r
    }
}

#[derive(Clone)]
pub struct PrimitiveSimdNotEq;

impl PrimitiveSimdImpl for PrimitiveSimdNotEq {
    fn vector_vector<T>(lhs: &PrimitiveColumn<T>, rhs: &PrimitiveColumn<T>) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialEq,
    {
        CommonPrimitiveImpl::compare_op(lhs, rhs, |a, b| a.neq(b))
    }

    fn vector_const<T>(lhs: &PrimitiveColumn<T>, rhs: T) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialEq,
    {
        CommonPrimitiveImpl::compare_op_scalar(lhs, rhs, |a, b| a.neq(b))
    }

    fn const_vector<T>(lhs: T, rhs: &PrimitiveColumn<T>) -> BooleanColumn
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialEq,
    {
        CommonPrimitiveImpl::compare_op_scalar(rhs, lhs, |a, b| a.neq(b))
    }
}

#[derive(Clone)]
pub struct BooleanSimdNotEq;

impl BooleanSimdImpl for BooleanSimdNotEq {
    fn vector_vector(lhs: &BooleanColumn, rhs: &BooleanColumn) -> BooleanColumn {
        CommonBooleanImpl::compare_op(lhs, rhs, |a, b| a ^ b)
    }

    fn vector_const(lhs: &BooleanColumn, rhs: bool) -> BooleanColumn {
        if rhs {
            CommonBooleanImpl::compare_op_scalar(lhs, rhs, |a, _| !a)
        } else {
            lhs.clone()
        }
    }

    fn const_vector(lhs: bool, rhs: &BooleanColumn) -> BooleanColumn {
        if lhs {
            CommonBooleanImpl::compare_op_scalar(rhs, lhs, |a, _| !a)
        } else {
            rhs.clone()
        }
    }
}
