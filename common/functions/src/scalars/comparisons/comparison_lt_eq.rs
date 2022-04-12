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
use common_arrow::arrow::compute::comparison::Simd8PartialOrd;
use common_datavalues::prelude::*;
use num::traits::AsPrimitive;

use super::comparison::ComparisonFunctionCreator;
use super::comparison::ComparisonImpl;
use super::comparison_gt_eq::BooleanSimdGtEq;
use super::utils::*;
use crate::scalars::EvalContext;
use crate::scalars::FunctionContext;

pub type ComparisonLtEqFunction = ComparisonFunctionCreator<ComparisonLtEqImpl>;

#[derive(Clone)]
pub struct ComparisonLtEqImpl;

impl ComparisonImpl for ComparisonLtEqImpl {
    type BooleanSimd = BooleanSimdLtEq;

    fn eval_simd<T>(l: T::Simd, r: T::Simd) -> u8
    where
        T: PrimitiveType + Simd8,
        T::Simd: Simd8PartialOrd,
    {
        l.lt_eq(r)
    }

    fn eval_primitive<L, R, M>(l: L::RefType<'_>, r: R::RefType<'_>, _ctx: &mut EvalContext) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType,
    {
        l.to_owned_scalar().as_().le(&r.to_owned_scalar().as_())
    }

    fn eval_binary(l: &[u8], r: &[u8], _ctx: &mut EvalContext) -> bool {
        l <= r
    }
}

#[derive(Clone)]
pub struct BooleanSimdLtEq;

impl BooleanSimdImpl for BooleanSimdLtEq {
    fn vector_vector(lhs: &BooleanColumn, rhs: &BooleanColumn) -> BooleanColumn {
        CommonBooleanOp::compare_op(lhs, rhs, |a, b| !a | b)
    }

    fn vector_const(lhs: &BooleanColumn, rhs: bool) -> BooleanColumn {
        if rhs {
            let all_ones = !0;
            CommonBooleanOp::compare_op_scalar(lhs, rhs, |_, _| all_ones)
        } else {
            CommonBooleanOp::compare_op_scalar(lhs, rhs, |a, _| !a)
        }
    }

    fn const_vector(lhs: bool, rhs: &BooleanColumn) -> BooleanColumn {
        BooleanSimdGtEq::vector_const(rhs, lhs)
    }
}
