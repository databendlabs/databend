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

use databend_common_column::bitmap::Bitmap;

use crate::EvalContext;
use crate::types::nullable::NullableColumn;
use crate::types::*;
use crate::values::Value;

pub trait VectorizedFn0<O: AccessType> = Fn(&mut EvalContext) -> Value<O> + Copy + Send + Sync;

pub trait VectorizedFn1<I1, O: AccessType> =
    Fn(Value<I1>, &mut EvalContext) -> Value<O> + Copy + Send + Sync;

pub trait VectorizedFn2<I1, I2, O: AccessType> =
    Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<O> + Copy + Send + Sync;

pub trait VectorizedFn3<I1, I2, I3, O: AccessType> =
    Fn(Value<I1>, Value<I2>, Value<I3>, &mut EvalContext) -> Value<O> + Copy + Send + Sync;

pub trait VectorizedFn4<I1, I2, I3, I4, O: AccessType> = Fn(Value<I1>, Value<I2>, Value<I3>, Value<I4>, &mut EvalContext) -> Value<O>
    + Copy
    + Send
    + Sync;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PartialEvalPolicy {
    /// Evaluate every row regardless of `EvalContext::validity`.
    #[default]
    EvaluateAll,
    /// Skip rows excluded by `EvalContext::validity` and write the output type's default payload.
    /// This is only valid when inactive payloads and skipped function calls are unobservable.
    SkipInactiveRows,
}

impl PartialEvalPolicy {
    /// Return a sparse active-row bitmap when row-level evaluation can be skipped.
    pub fn active_rows(self, ctx: &EvalContext) -> Option<Bitmap> {
        match self {
            PartialEvalPolicy::EvaluateAll => None,
            PartialEvalPolicy::SkipInactiveRows if ctx.num_rows == 0 => Some(Bitmap::new()),
            PartialEvalPolicy::SkipInactiveRows => ctx
                .validity
                .as_ref()
                .filter(|validity| validity.null_count() > 0)
                .cloned(),
        }
    }
}

pub fn vectorize_1_arg<I1: AccessType, O: ReturnType>(
    func: impl Fn(I1::ScalarRef<'_>, &mut EvalContext) -> O::Scalar + Copy + Send + Sync,
) -> impl VectorizedFn1<I1, O> {
    vectorize_1_arg_with_policy(PartialEvalPolicy::EvaluateAll, func)
}

pub fn vectorize_1_arg_with_policy<I1: AccessType, O: ReturnType>(
    policy: PartialEvalPolicy,
    func: impl Fn(I1::ScalarRef<'_>, &mut EvalContext) -> O::Scalar + Copy + Send + Sync,
) -> impl VectorizedFn1<I1, O> {
    move |arg1, ctx| match arg1 {
        Value::Scalar(arg1) => {
            let active_rows = policy.active_rows(ctx);
            if active_rows
                .as_ref()
                .is_some_and(|validity| validity.null_count() == validity.len())
            {
                let mut builder = O::create_builder(1, ctx.generics);
                O::push_default(&mut builder);
                Value::Scalar(O::build_scalar(builder))
            } else {
                let result = func(I1::to_scalar_ref(&arg1), ctx);
                Value::Scalar(result)
            }
        }
        Value::Column(arg1) => {
            let Some(active_rows) = policy.active_rows(ctx) else {
                let generics = ctx.generics.to_vec();
                let iter = I1::iter_column(&arg1).map(|arg1| func(arg1, ctx));
                let col = O::column_from_iter(iter, &generics);
                return Value::Column(col);
            };

            let mut builder = O::create_builder(ctx.num_rows, ctx.generics);
            for (index, arg1) in I1::iter_column(&arg1).enumerate() {
                if active_rows.get_bit(index) {
                    let result = func(arg1, ctx);
                    O::push_item(&mut builder, O::to_scalar_ref(&result));
                } else {
                    O::push_default(&mut builder);
                }
            }
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn vectorize_2_arg<I1: AccessType, I2: AccessType, O: ReturnType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> O::Scalar
    + Copy
    + Send
    + Sync,
) -> impl VectorizedFn2<I1, I2, O> {
    vectorize_2_arg_with_policy(PartialEvalPolicy::EvaluateAll, func)
}

pub fn vectorize_2_arg_with_policy<I1: AccessType, I2: AccessType, O: ReturnType>(
    policy: PartialEvalPolicy,
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> O::Scalar
    + Copy
    + Send
    + Sync,
) -> impl VectorizedFn2<I1, I2, O> {
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (Value::Scalar(arg1), Value::Scalar(arg2)) => {
            let active_rows = policy.active_rows(ctx);
            if active_rows
                .as_ref()
                .is_some_and(|validity| validity.null_count() == validity.len())
            {
                let mut builder = O::create_builder(1, ctx.generics);
                O::push_default(&mut builder);
                Value::Scalar(O::build_scalar(builder))
            } else {
                let result = func(I1::to_scalar_ref(&arg1), I2::to_scalar_ref(&arg2), ctx);
                Value::Scalar(result)
            }
        }
        (Value::Scalar(arg1), Value::Column(arg2)) => {
            let active_rows = policy.active_rows(ctx);
            let generics = ctx.generics.to_vec();
            if let Some(active_rows) = active_rows {
                let mut builder = O::create_builder(ctx.num_rows, &generics);
                for (index, arg2) in I2::iter_column(&arg2).enumerate() {
                    if active_rows.get_bit(index) {
                        let arg1 = I1::to_scalar_ref(&arg1);
                        let result = func(arg1, arg2, ctx);
                        O::push_item(&mut builder, O::to_scalar_ref(&result));
                    } else {
                        O::push_default(&mut builder);
                    }
                }
                Value::Column(O::build_column(builder))
            } else {
                let iter = I2::iter_column(&arg2).map(|arg2| {
                    let arg1 = I1::to_scalar_ref(&arg1);
                    func(arg1, arg2, ctx)
                });
                let col = O::column_from_iter(iter, &generics);
                Value::Column(col)
            }
        }
        (Value::Column(arg1), Value::Scalar(arg2)) => {
            let active_rows = policy.active_rows(ctx);
            let generics = ctx.generics.to_vec();
            if let Some(active_rows) = active_rows {
                let mut builder = O::create_builder(ctx.num_rows, &generics);
                for (index, arg1) in I1::iter_column(&arg1).enumerate() {
                    if active_rows.get_bit(index) {
                        let arg2 = I2::to_scalar_ref(&arg2);
                        let result = func(arg1, arg2, ctx);
                        O::push_item(&mut builder, O::to_scalar_ref(&result));
                    } else {
                        O::push_default(&mut builder);
                    }
                }
                Value::Column(O::build_column(builder))
            } else {
                let iter = I1::iter_column(&arg1).map(|arg1| {
                    let arg2 = I2::to_scalar_ref(&arg2);
                    func(arg1, arg2, ctx)
                });
                let col = O::column_from_iter(iter, &generics);
                Value::Column(col)
            }
        }
        (Value::Column(arg1), Value::Column(arg2)) => {
            let active_rows = policy.active_rows(ctx);
            let generics = ctx.generics.to_vec();
            if let Some(active_rows) = active_rows {
                let mut builder = O::create_builder(ctx.num_rows, &generics);
                for (index, (arg1, arg2)) in I1::iter_column(&arg1)
                    .zip(I2::iter_column(&arg2))
                    .enumerate()
                {
                    if active_rows.get_bit(index) {
                        let result = func(arg1, arg2, ctx);
                        O::push_item(&mut builder, O::to_scalar_ref(&result));
                    } else {
                        O::push_default(&mut builder);
                    }
                }
                Value::Column(O::build_column(builder))
            } else {
                let iter = I1::iter_column(&arg1)
                    .zip(I2::iter_column(&arg2))
                    .map(|(arg1, arg2)| func(arg1, arg2, ctx));
                let col = O::column_from_iter(iter, &generics);
                Value::Column(col)
            }
        }
    }
}

pub fn vectorize_3_arg<I1: AccessType, I2: AccessType, I3: AccessType, O: ReturnType>(
    func: impl Fn(
        I1::ScalarRef<'_>,
        I2::ScalarRef<'_>,
        I3::ScalarRef<'_>,
        &mut EvalContext,
    ) -> O::Scalar
    + Copy
    + Send
    + Sync,
) -> impl VectorizedFn3<I1, I2, I3, O> {
    move |arg1, arg2, arg3, ctx| {
        let generics = ctx.generics.to_vec();

        let input_all_scalars =
            arg1.as_scalar().is_some() && arg2.as_scalar().is_some() && arg3.as_scalar().is_some();
        let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

        let iter = (0..process_rows).map(|index| {
            let arg1 = unsafe { arg1.index_unchecked(index) };
            let arg2 = unsafe { arg2.index_unchecked(index) };
            let arg3 = unsafe { arg3.index_unchecked(index) };
            func(arg1, arg2, arg3, ctx)
        });
        let col = O::column_from_iter(iter, &generics);
        if input_all_scalars {
            Value::Scalar(unsafe { O::index_column_unchecked_scalar(&col, 0) })
        } else {
            Value::Column(col)
        }
    }
}

pub fn vectorize_4_arg<
    I1: AccessType,
    I2: AccessType,
    I3: AccessType,
    I4: AccessType,
    O: ReturnType,
>(
    func: impl Fn(
        I1::ScalarRef<'_>,
        I2::ScalarRef<'_>,
        I3::ScalarRef<'_>,
        I4::ScalarRef<'_>,
        &mut EvalContext,
    ) -> O::Scalar
    + Copy
    + Send
    + Sync,
) -> impl VectorizedFn4<I1, I2, I3, I4, O> {
    move |arg1, arg2, arg3, arg4, ctx| {
        let generics = ctx.generics.to_vec();

        let input_all_scalars = arg1.as_scalar().is_some()
            && arg2.as_scalar().is_some()
            && arg3.as_scalar().is_some()
            && arg4.as_scalar().is_some();
        let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

        let iter = (0..process_rows).map(|index| {
            let arg1 = unsafe { arg1.index_unchecked(index) };
            let arg2 = unsafe { arg2.index_unchecked(index) };
            let arg3 = unsafe { arg3.index_unchecked(index) };
            let arg4 = unsafe { arg4.index_unchecked(index) };
            func(arg1, arg2, arg3, arg4, ctx)
        });
        let col = O::column_from_iter(iter, &generics);
        if input_all_scalars {
            Value::Scalar(unsafe { O::index_column_unchecked_scalar(&col, 0) })
        } else {
            Value::Column(col)
        }
    }
}

pub fn vectorize_with_builder_1_arg<I1: AccessType, O: ReturnType>(
    func: impl Fn(I1::ScalarRef<'_>, &mut O::ColumnBuilder, &mut EvalContext) + Copy + Send + Sync,
) -> impl VectorizedFn1<I1, O> {
    move |arg1, ctx| {
        let generics = ctx.generics;
        match arg1 {
            Value::Scalar(arg1) => {
                let mut builder = O::create_builder(1, generics);
                func(I1::to_scalar_ref(&arg1), &mut builder, ctx);
                Value::Scalar(O::build_scalar(builder))
            }
            Value::Column(arg1) => {
                let mut builder = O::create_builder(ctx.num_rows, generics);
                for arg1 in I1::iter_column(&arg1) {
                    func(arg1, &mut builder, ctx);
                }
                Value::Column(O::build_column(builder))
            }
        }
    }
}

pub fn vectorize_with_builder_2_arg<I1: AccessType, I2: AccessType, O: ReturnType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut O::ColumnBuilder, &mut EvalContext)
    + Copy
    + Send
    + Sync,
) -> impl VectorizedFn2<I1, I2, O> {
    move |arg1, arg2, ctx| {
        let generics = ctx.generics.to_vec();

        let input_all_scalars = arg1.as_scalar().is_some() && arg2.as_scalar().is_some();
        let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

        let mut builder = O::create_builder(process_rows, &generics);
        for index in 0..process_rows {
            let arg1 = unsafe { arg1.index_unchecked(index) };
            let arg2 = unsafe { arg2.index_unchecked(index) };
            func(arg1, arg2, &mut builder, ctx);
        }
        if input_all_scalars {
            Value::Scalar(O::build_scalar(builder))
        } else {
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn vectorize_with_builder_3_arg<
    I1: AccessType,
    I2: AccessType,
    I3: AccessType,
    O: ReturnType,
>(
    func: impl Fn(
        I1::ScalarRef<'_>,
        I2::ScalarRef<'_>,
        I3::ScalarRef<'_>,
        &mut O::ColumnBuilder,
        &mut EvalContext,
    ) + Copy
    + Send
    + Sync,
) -> impl VectorizedFn3<I1, I2, I3, O> {
    move |arg1, arg2, arg3, ctx| {
        let generics = ctx.generics.to_vec();

        let input_all_scalars =
            arg1.as_scalar().is_some() && arg2.as_scalar().is_some() && arg3.as_scalar().is_some();
        let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

        let mut builder = O::create_builder(process_rows, &generics);
        for index in 0..process_rows {
            let arg1 = unsafe { arg1.index_unchecked(index) };
            let arg2 = unsafe { arg2.index_unchecked(index) };
            let arg3 = unsafe { arg3.index_unchecked(index) };
            func(arg1, arg2, arg3, &mut builder, ctx);
        }
        if input_all_scalars {
            Value::Scalar(O::build_scalar(builder))
        } else {
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn vectorize_with_builder_4_arg<
    I1: AccessType,
    I2: AccessType,
    I3: AccessType,
    I4: AccessType,
    O: ReturnType,
>(
    func: impl Fn(
        I1::ScalarRef<'_>,
        I2::ScalarRef<'_>,
        I3::ScalarRef<'_>,
        I4::ScalarRef<'_>,
        &mut O::ColumnBuilder,
        &mut EvalContext,
    ) + Copy
    + Send
    + Sync,
) -> impl VectorizedFn4<I1, I2, I3, I4, O> {
    move |arg1, arg2, arg3, arg4, ctx| {
        let generics = ctx.generics.to_vec();

        let input_all_scalars = arg1.as_scalar().is_some()
            && arg2.as_scalar().is_some()
            && arg3.as_scalar().is_some()
            && arg4.as_scalar().is_some();
        let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

        let mut builder = O::create_builder(process_rows, &generics);
        for index in 0..process_rows {
            let arg1 = unsafe { arg1.index_unchecked(index) };
            let arg2 = unsafe { arg2.index_unchecked(index) };
            let arg3 = unsafe { arg3.index_unchecked(index) };
            let arg4 = unsafe { arg4.index_unchecked(index) };
            func(arg1, arg2, arg3, arg4, &mut builder, ctx);
        }
        if input_all_scalars {
            Value::Scalar(O::build_scalar(builder))
        } else {
            Value::Column(O::build_column(builder))
        }
    }
}

pub fn passthrough_nullable_1_arg<I1: AccessType, O: ReturnType>(
    func: impl VectorizedFn1<I1, O>,
) -> impl VectorizedFn1<NullableType<I1>, NullableType<O>> {
    move |arg1, ctx| {
        let mut args_validity = arg1.validity(ctx.num_rows);
        if let Some(validity) = ctx.validity.as_ref() {
            args_validity = &args_validity & validity;
        }
        ctx.validity = Some(args_validity.clone());
        match arg1.value() {
            Some(arg1) => {
                let out = func(arg1, ctx);

                match out {
                    Value::Column(out) => {
                        Value::Column(NullableColumn::new_unchecked(out, args_validity))
                    }
                    Value::Scalar(out) => Value::Scalar(Some(out)),
                }
            }
            _ => Value::Scalar(None),
        }
    }
}

pub fn passthrough_nullable_2_arg<I1: AccessType, I2: AccessType, O: ReturnType>(
    func: impl VectorizedFn2<I1, I2, O>,
) -> impl VectorizedFn2<NullableType<I1>, NullableType<I2>, NullableType<O>> {
    move |arg1, arg2, ctx| {
        let mut args_validity = arg1.validity(ctx.num_rows);
        args_validity = &args_validity & &arg2.validity(ctx.num_rows);
        if let Some(validity) = ctx.validity.as_ref() {
            args_validity = &args_validity & validity;
        }

        ctx.validity = Some(args_validity.clone());
        match (arg1.value(), arg2.value()) {
            (Some(arg1), Some(arg2)) => {
                let out = func(arg1, arg2, ctx);

                match out {
                    Value::Column(out) => {
                        Value::Column(NullableColumn::new_unchecked(out, args_validity))
                    }
                    Value::Scalar(out) => Value::Scalar(Some(out)),
                }
            }
            _ => Value::Scalar(None),
        }
    }
}

pub fn passthrough_nullable_3_arg<I1: AccessType, I2: AccessType, I3: AccessType, O: ReturnType>(
    func: impl VectorizedFn3<I1, I2, I3, O>,
) -> impl VectorizedFn3<NullableType<I1>, NullableType<I2>, NullableType<I3>, NullableType<O>> {
    move |arg1, arg2, arg3, ctx| {
        let mut args_validity = arg1.validity(ctx.num_rows);
        args_validity = &args_validity & &arg2.validity(ctx.num_rows);
        args_validity = &args_validity & &arg3.validity(ctx.num_rows);
        if let Some(validity) = ctx.validity.as_ref() {
            args_validity = &args_validity & validity;
        }
        ctx.validity = Some(args_validity.clone());
        match (arg1.value(), arg2.value(), arg3.value()) {
            (Some(arg1), Some(arg2), Some(arg3)) => {
                let out = func(arg1, arg2, arg3, ctx);

                match out {
                    Value::Column(out) => {
                        Value::Column(NullableColumn::new_unchecked(out, args_validity))
                    }
                    Value::Scalar(out) => Value::Scalar(Some(out)),
                }
            }
            _ => Value::Scalar(None),
        }
    }
}

pub fn passthrough_nullable_4_arg<
    I1: AccessType,
    I2: AccessType,
    I3: AccessType,
    I4: AccessType,
    O: ReturnType,
>(
    func: impl VectorizedFn4<I1, I2, I3, I4, O>,
) -> impl VectorizedFn4<
    NullableType<I1>,
    NullableType<I2>,
    NullableType<I3>,
    NullableType<I4>,
    NullableType<O>,
> {
    move |arg1, arg2, arg3, arg4, ctx| {
        let mut args_validity = arg1.validity(ctx.num_rows);
        args_validity = &args_validity & &arg2.validity(ctx.num_rows);
        args_validity = &args_validity & &arg3.validity(ctx.num_rows);
        args_validity = &args_validity & &arg4.validity(ctx.num_rows);
        if let Some(validity) = ctx.validity.as_ref() {
            args_validity = &args_validity & validity;
        }
        ctx.validity = Some(args_validity.clone());
        match (arg1.value(), arg2.value(), arg3.value(), arg4.value()) {
            (Some(arg1), Some(arg2), Some(arg3), Some(arg4)) => {
                let out = func(arg1, arg2, arg3, arg4, ctx);

                match out {
                    Value::Column(out) => {
                        Value::Column(NullableColumn::new_unchecked(out, args_validity))
                    }
                    Value::Scalar(out) => Value::Scalar(Some(out)),
                }
            }
            _ => Value::Scalar(None),
        }
    }
}

pub fn combine_nullable_1_arg<I1: AccessType, O: ReturnType>(
    func: impl Fn(Value<I1>, &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync,
) -> impl VectorizedFn1<NullableType<I1>, NullableType<O>> {
    move |arg1, ctx| {
        let mut args_validity = arg1.validity(ctx.num_rows);
        if let Some(validity) = ctx.validity.as_ref() {
            args_validity = &args_validity & validity;
        }
        ctx.validity = Some(args_validity.clone());
        match arg1.value() {
            Some(arg1) => {
                let out = func(arg1, ctx);

                match out {
                    Value::Column(out) => Value::Column(NullableColumn::new_unchecked(
                        out.column,
                        &args_validity & &out.validity,
                    )),
                    Value::Scalar(out) => Value::Scalar(out),
                }
            }
            _ => Value::Scalar(None),
        }
    }
}

pub fn combine_nullable_2_arg<I1: AccessType, I2: AccessType, O: ReturnType>(
    func: impl Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync,
) -> impl VectorizedFn2<NullableType<I1>, NullableType<I2>, NullableType<O>> {
    move |arg1, arg2, ctx| {
        let mut args_validity = arg1.validity(ctx.num_rows);
        args_validity = &args_validity & &arg2.validity(ctx.num_rows);
        if let Some(validity) = ctx.validity.as_ref() {
            args_validity = &args_validity & validity;
        }
        ctx.validity = Some(args_validity.clone());
        match (arg1.value(), arg2.value()) {
            (Some(arg1), Some(arg2)) => {
                let out = func(arg1, arg2, ctx);

                match out {
                    Value::Column(out) => Value::Column(NullableColumn::new_unchecked(
                        out.column,
                        &args_validity & &out.validity,
                    )),
                    Value::Scalar(out) => Value::Scalar(out),
                }
            }
            _ => Value::Scalar(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::FunctionContext;

    fn eval_context<'a>(func_ctx: &'a FunctionContext, validity: Bitmap) -> EvalContext<'a> {
        EvalContext {
            generics: &[],
            num_rows: validity.len(),
            func_ctx,
            validity: Some(validity),
            errors: None,
            suppress_error: false,
            strict_eval: false,
        }
    }

    fn assert_boolean_column(value: Value<BooleanType>, expected: &[bool]) {
        let Value::Column(column) = value else {
            unreachable!()
        };
        assert_eq!(column.iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn test_partial_eval_policy_controls_row_evaluation() {
        let calls = AtomicUsize::new(0);
        let func = |value: bool, _ctx: &mut EvalContext| {
            calls.fetch_add(1, Ordering::Relaxed);
            !value
        };
        let func_ctx = FunctionContext::default();
        let input = Value::<BooleanType>::Column(Bitmap::new_constant(false, 4));

        let mut evaluate_all_ctx =
            eval_context(&func_ctx, Bitmap::from_iter([true, false, true, false]));
        let evaluate_all = vectorize_1_arg_with_policy::<BooleanType, BooleanType>(
            PartialEvalPolicy::EvaluateAll,
            func,
        )(input.clone(), &mut evaluate_all_ctx);
        assert_boolean_column(evaluate_all, &[true, true, true, true]);
        assert_eq!(calls.load(Ordering::Relaxed), 4);

        calls.store(0, Ordering::Relaxed);
        let mut skip_inactive_ctx =
            eval_context(&func_ctx, Bitmap::from_iter([true, false, true, false]));
        let skip_inactive = vectorize_1_arg_with_policy::<BooleanType, BooleanType>(
            PartialEvalPolicy::SkipInactiveRows,
            func,
        )(input, &mut skip_inactive_ctx);
        assert_boolean_column(skip_inactive, &[true, false, true, false]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_skip_inactive_rows_for_binary_function() {
        let func_ctx = FunctionContext::default();
        let validity = Bitmap::from_iter([true, false, true, false]);
        let column = || Value::<BooleanType>::Column(Bitmap::new_constant(false, 4));
        let scalar = || Value::<BooleanType>::Scalar(false);

        let calls = AtomicUsize::new(0);
        let mut ctx = eval_context(&func_ctx, validity.clone());
        let result = vectorize_2_arg_with_policy::<BooleanType, BooleanType, BooleanType>(
            PartialEvalPolicy::SkipInactiveRows,
            |_, _, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                true
            },
        )(column(), scalar(), &mut ctx);
        assert_boolean_column(result, &[true, false, true, false]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_partial_eval_policy_handles_scalar_inputs() {
        let calls = AtomicUsize::new(0);
        let func = |value: bool, _ctx: &mut EvalContext| {
            calls.fetch_add(1, Ordering::Relaxed);
            !value
        };
        let func_ctx = FunctionContext::default();
        let vectorized = vectorize_1_arg_with_policy::<BooleanType, BooleanType>(
            PartialEvalPolicy::SkipInactiveRows,
            func,
        );

        let mut partially_active_ctx =
            eval_context(&func_ctx, Bitmap::from_iter([false, true, false]));
        let partially_active = vectorized(
            Value::<BooleanType>::Scalar(false),
            &mut partially_active_ctx,
        );
        assert_eq!(partially_active, Value::Scalar(true));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        calls.store(0, Ordering::Relaxed);
        let mut inactive_ctx = eval_context(&func_ctx, Bitmap::new_constant(false, 3));
        let inactive = vectorized(Value::<BooleanType>::Scalar(false), &mut inactive_ctx);
        assert_eq!(inactive, Value::Scalar(false));
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        let mut empty_ctx = eval_context(&func_ctx, Bitmap::new());
        empty_ctx.validity = None;
        let empty = vectorized(Value::<BooleanType>::Scalar(false), &mut empty_ctx);
        assert_eq!(empty, Value::Scalar(false));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
