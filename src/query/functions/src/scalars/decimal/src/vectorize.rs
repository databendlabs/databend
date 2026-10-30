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

//! Builder-based vectorization that skips rows excluded by
//! `EvalContext::validity` (`PartialEvalPolicy::SkipInactiveRows`). A scalar
//! argument is evaluated once with the mask lifted, and its error is reported
//! for every active row.
//!
//! The expression framework only offers this policy for non-builder
//! vectorizers; these belong there eventually.

use databend_common_expression::EvalContext;
use databend_common_expression::Value;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;

// Callers must be pure. Only the masked path differs from the framework's
// default builder loop; all-visible columns retain that loop unchanged.
fn inactive_output<O: ReturnType>(ctx: &EvalContext, scalar: bool) -> Value<O> {
    let rows = if scalar { 1 } else { ctx.num_rows };
    let mut builder = O::create_builder(rows, ctx.generics);
    for _ in 0..rows {
        O::push_default(&mut builder);
    }
    if scalar {
        Value::Scalar(O::build_scalar(builder))
    } else {
        Value::Column(O::build_column(builder))
    }
}

fn scalar_scope<O: ReturnType>(
    ctx: &mut EvalContext,
    evaluate: impl FnOnce(&mut EvalContext) -> Value<O>,
) -> Value<O> {
    let validity = ctx.validity.take();
    let previous_errors = ctx.errors.take();
    // The single value is needed by at least one active row. Its builder uses
    // index zero, so collect its error before restoring the real row mask.
    let output = evaluate(ctx);
    let scalar_error = ctx.errors.take();
    ctx.validity = validity;
    ctx.errors = previous_errors;
    if let Some((_, message)) = scalar_error {
        for row in 0..ctx.num_rows {
            ctx.set_error(row, message.as_str());
        }
    }
    output
}

pub(crate) fn skip_inactive_with_builder_1_arg<I: AccessType, O: ReturnType>(
    func: impl Fn(I::ScalarRef<'_>, &mut O::ColumnBuilder, &mut EvalContext) + Copy + Send + Sync,
) -> impl Fn(Value<I>, &mut EvalContext) -> Value<O> + Copy + Send + Sync {
    move |arg, ctx| {
        let scalar = arg.as_scalar().is_some();
        if ctx.num_rows == 0 && scalar {
            return inactive_output::<O>(ctx, true);
        }
        let Some(active) = ctx
            .validity
            .as_ref()
            .filter(|v| v.null_count() != 0)
            .cloned()
        else {
            return vectorize_with_builder_1_arg::<I, O>(func)(arg, ctx);
        };
        if active.null_count() == active.len() {
            return inactive_output::<O>(ctx, scalar);
        }
        if scalar {
            return scalar_scope(ctx, |ctx| {
                vectorize_with_builder_1_arg::<I, O>(func)(arg, ctx)
            });
        }
        let Value::Column(column) = arg else {
            unreachable!()
        };
        let mut builder = O::create_builder(ctx.num_rows, ctx.generics);
        for (row, value) in I::iter_column(&column).enumerate() {
            if active.get_bit(row) {
                func(value, &mut builder, ctx);
            } else {
                O::push_default(&mut builder);
            }
        }
        Value::Column(O::build_column(builder))
    }
}

pub(crate) fn skip_inactive_with_builder_2_arg<I: AccessType, J: AccessType, O: ReturnType>(
    func: impl Fn(I::ScalarRef<'_>, J::ScalarRef<'_>, &mut O::ColumnBuilder, &mut EvalContext)
    + Copy
    + Send
    + Sync,
) -> impl Fn(Value<I>, Value<J>, &mut EvalContext) -> Value<O> + Copy + Send + Sync {
    move |a, b, ctx| {
        let scalar = a.as_scalar().is_some() && b.as_scalar().is_some();
        if ctx.num_rows == 0 && scalar {
            return inactive_output::<O>(ctx, true);
        }
        let Some(active) = ctx
            .validity
            .as_ref()
            .filter(|v| v.null_count() != 0)
            .cloned()
        else {
            return vectorize_with_builder_2_arg::<I, J, O>(func)(a, b, ctx);
        };
        if active.null_count() == active.len() {
            return inactive_output::<O>(ctx, scalar);
        }
        if scalar {
            return scalar_scope(ctx, |ctx| {
                vectorize_with_builder_2_arg::<I, J, O>(func)(a, b, ctx)
            });
        }
        let mut builder = O::create_builder(ctx.num_rows, ctx.generics);
        for row in 0..ctx.num_rows {
            if active.get_bit(row) {
                func(
                    a.index(row).unwrap(),
                    b.index(row).unwrap(),
                    &mut builder,
                    ctx,
                );
            } else {
                O::push_default(&mut builder);
            }
        }
        Value::Column(O::build_column(builder))
    }
}
