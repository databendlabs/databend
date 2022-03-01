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

use common_datavalues::PrimitiveType;
use common_datavalues::ScalarRef;
use num::traits::AsPrimitive;

use super::comparison::ComparisonFunctionCreator;
use super::comparison::ComparisonImpl;
use crate::scalars::EvalContext;

#[derive(Clone)]
pub struct ComparisonGtEqImpl;

impl ComparisonImpl for ComparisonGtEqImpl {
    fn eval_primitive<L, R, M>(l: L::RefType<'_>, r: R::RefType<'_>, _ctx: &mut EvalContext) -> bool
    where
        L: PrimitiveType + AsPrimitive<M>,
        R: PrimitiveType + AsPrimitive<M>,
        M: PrimitiveType,
    {
        l.to_owned_scalar().as_().ge(&r.to_owned_scalar().as_())
    }

    fn eval_binary(l: &[u8], r: &[u8], _ctx: &mut EvalContext) -> bool {
        l >= r
    }

    fn eval_bool(l: bool, r: bool, _ctx: &mut EvalContext) -> bool {
        l | !r
    }
}

pub type ComparisonGtEqFunction = ComparisonFunctionCreator<ComparisonGtEqImpl>;
