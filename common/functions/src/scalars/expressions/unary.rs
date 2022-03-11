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

use common_datavalues::prelude::*;
use common_exception::Result;

use super::EvalContext;

pub trait ScalarUnaryFunction<L: Scalar, O: Scalar> {
    fn eval(&self, l: L::RefType<'_>, _ctx: &mut EvalContext) -> O;
}

/// Blanket implementation for all binary expression functions
impl<L: Scalar, O: Scalar, F> ScalarUnaryFunction<L, O> for F
where F: Fn(L::RefType<'_>, &mut EvalContext) -> O
{
    fn eval(&self, i1: L::RefType<'_>, ctx: &mut EvalContext) -> O {
        self(i1, ctx)
    }
}

pub fn scalar_unary_op<L: Scalar, O: Scalar, F>(
    l: &ColumnRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::ColumnType>
where
    F: ScalarUnaryFunction<L, O>,
{
    let left = Series::check_get_scalar::<L>(l)?;
    let it = left.scalar_iter().map(|a| f.eval(a, ctx));
    let result = <O as Scalar>::ColumnType::from_owned_iterator(it);

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}
