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

pub fn vectorize_1_arg<I1: AccessType, O: ReturnType>(
    func: impl Fn(I1::ScalarRef<'_>, &mut EvalContext) -> O::Scalar + Copy + Send + Sync,
) -> impl VectorizedFn1<I1, O> {
    move |arg1, ctx| match arg1 {
        Value::Scalar(arg1) => {
            let result = func(I1::to_scalar_ref(&arg1), ctx);
            Value::Scalar(result)
        }
        Value::Column(arg1) => {
            let generics = ctx.generics.to_vec();
            let iter = I1::iter_column(&arg1).map(|arg1| func(arg1, ctx));
            let col = O::column_from_iter(iter, &generics);
            Value::Column(col)
        }
    }
}

pub fn vectorize_2_arg<I1: AccessType, I2: AccessType, O: ReturnType>(
    func: impl Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> O::Scalar
    + Copy
    + Send
    + Sync,
) -> impl VectorizedFn2<I1, I2, O> {
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (Value::Scalar(arg1), Value::Scalar(arg2)) => {
            let result = func(I1::to_scalar_ref(&arg1), I2::to_scalar_ref(&arg2), ctx);
            Value::Scalar(result)
        }
        (Value::Scalar(arg1), Value::Column(arg2)) => {
            let generics = ctx.generics.to_vec();
            let iter = I2::iter_column(&arg2).map(|arg2| {
                let arg1 = I1::to_scalar_ref(&arg1);
                func(arg1, arg2, ctx)
            });
            let col = O::column_from_iter(iter, &generics);
            Value::Column(col)
        }
        (Value::Column(arg1), Value::Scalar(arg2)) => {
            let generics = ctx.generics.to_vec();
            let iter = I1::iter_column(&arg1).map(|arg1| {
                let arg2 = I2::to_scalar_ref(&arg2);
                func(arg1, arg2, ctx)
            });
            let col = O::column_from_iter(iter, &generics);
            Value::Column(col)
        }
        (Value::Column(arg1), Value::Column(arg2)) => {
            let generics = ctx.generics.to_vec();
            let iter = I1::iter_column(&arg1)
                .zip(I2::iter_column(&arg2))
                .map(|(arg1, arg2)| func(arg1, arg2, ctx));
            let col = O::column_from_iter(iter, &generics);
            Value::Column(col)
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
