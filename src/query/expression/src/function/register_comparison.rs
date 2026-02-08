// Copyright 2021 Datafuse Labs
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

use crate::EvalContext;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::FunctionRegistry;
use crate::types::*;
use crate::values::Value;

impl FunctionRegistry {
    pub fn register_comparison_2_arg<I1: ArgType, I2: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<BooleanType>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> bool
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        for<'a> I1::ScalarRef<'a>: Copy,
        for<'a> I2::ScalarRef<'a>: Copy,
    {
        self.register_passthrough_nullable_2_arg::<I1, I2, BooleanType, _, _>(
            name,
            calc_domain,
            vectorize_cmp_2_arg(func),
        )
    }
}

pub fn vectorize_cmp_2_arg<I1: AccessType, I2: AccessType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> bool + Copy + Send + Sync,
) -> impl Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<BooleanType> + Copy + Send + Sync
where
    for<'a> I1::ScalarRef<'a>: Copy,
    for<'a> I2::ScalarRef<'a>: Copy,
{
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (Value::Scalar(arg1), Value::Scalar(arg2)) => Value::Scalar(func(
            I1::to_scalar_ref(&arg1),
            I2::to_scalar_ref(&arg2),
            ctx,
        )),
        (Value::Column(arg1), Value::Scalar(arg2)) => {
            let arg2 = I2::to_scalar_ref(&arg2);
            let col = Bitmap::collect_bool(ctx.num_rows, |idx| {
                let arg1 = unsafe { I1::index_column_unchecked(&arg1, idx) };
                func(arg1, arg2, ctx)
            });

            Value::Column(col)
        }
        (Value::Scalar(arg1), Value::Column(arg2)) => {
            let arg1 = I1::to_scalar_ref(&arg1);
            let col = Bitmap::collect_bool(ctx.num_rows, |idx| {
                let arg2 = unsafe { I2::index_column_unchecked(&arg2, idx) };
                func(arg1, arg2, ctx)
            });

            Value::Column(col)
        }
        (Value::Column(arg1), Value::Column(arg2)) => {
            let col = Bitmap::collect_bool(ctx.num_rows, |idx| {
                let arg1 = unsafe { I1::index_column_unchecked(&arg1, idx) };
                let arg2 = unsafe { I2::index_column_unchecked(&arg2, idx) };
                func(arg1, arg2, ctx)
            });
            Value::Column(col)
        }
    }
}
