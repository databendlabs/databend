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

use std::ops::Rem;

use databend_common_expression::EvalContext;
use databend_common_expression::Value;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::number::*;
use num_traits::AsPrimitive;
use strength_reduce::StrengthReducedU8;
use strength_reduce::StrengthReducedU16;
use strength_reduce::StrengthReducedU32;
use strength_reduce::StrengthReducedU64;

pub fn vectorize_modulo<L, R, M, O>()
-> impl Fn(Value<NumberType<L>>, Value<NumberType<R>>, &mut EvalContext) -> Value<NumberType<O>> + Copy
where
    L: Number + AsPrimitive<M>,
    R: Number + AsPrimitive<M>,
    M: Number + AsPrimitive<O> + Rem<Output = M> + RemScalar<L, R, O> + ModuloValue,
    O: Number,
{
    move |arg1, arg2, ctx| {
        let apply = |lhs: &L, rhs: &R, builder: &mut Vec<O>, ctx: &mut EvalContext| {
            push_modulo_result::<L, R, M, O>(lhs, rhs, builder, ctx);
        };

        match (arg1, arg2) {
            (Value::Column(lhs), Value::Scalar(rhs)) => {
                <M as RemScalar<L, R, O>>::rem_scalar(lhs.iter(), &rhs, ctx)
            }
            (Value::Scalar(lhs), Value::Scalar(rhs)) => {
                let mut builder: Vec<O> = Vec::with_capacity(1);
                apply(&lhs, &rhs, &mut builder, ctx);
                Value::Scalar(NumberType::<O>::build_scalar(builder))
            }
            (Value::Scalar(arg1), Value::Column(arg2)) => {
                let mut builder: Vec<O> = Vec::with_capacity(arg2.len());
                for val in arg2.iter() {
                    apply(&arg1, val, &mut builder, ctx);
                }
                Value::Column(builder.into())
            }
            (Value::Column(arg1), Value::Column(arg2)) => {
                let mut builder: Vec<O> = Vec::with_capacity(arg2.len());
                let iter = arg1.iter().zip(arg2.iter());
                for (val1, val2) in iter {
                    apply(val1, val2, &mut builder, ctx);
                }
                Value::Column(builder.into())
            }
        }
    }
}

fn push_modulo_result<L, R, M, O>(lhs: &L, rhs: &R, output: &mut Vec<O>, ctx: &mut EvalContext)
where
    L: Number + AsPrimitive<M>,
    R: Number + AsPrimitive<M>,
    M: Number + AsPrimitive<O> + Rem<Output = M> + ModuloValue,
    O: Number,
{
    if std::intrinsics::unlikely(*rhs == R::default()) {
        ctx.set_error(output.len(), "Division by zero");
        output.push(O::default());
        return;
    }

    let rhs: M = rhs.as_();
    let lhs: M = lhs.as_();
    if std::intrinsics::unlikely(M::is_signed_min_modulo_minus_one(lhs, rhs)) {
        // Mathematically MIN % -1 is 0. Rust must guard this case before
        // lowering to LLVM because the machine signed remainder instruction has
        // undefined behavior here, so spell out the value instead of letting the
        // generated code panic.
        output.push(O::default());
        return;
    }

    output.push((lhs % rhs).as_());
}

pub trait ModuloValue: Number {
    fn is_signed_min_modulo_minus_one(_lhs: Self, _rhs: Self) -> bool {
        false
    }
}

pub trait RemScalar<L: Number, R: Number, O: Number>: Number {
    fn rem_scalar<'a>(
        left: impl ExactSizeIterator<Item = &'a L>,
        other: &R,
        ctx: &mut EvalContext,
    ) -> Value<NumberType<O>>
    where
        L: 'a + AsPrimitive<Self>,
        R: AsPrimitive<Self>,
        Self: AsPrimitive<O> + Rem<Output = Self> + ModuloValue,
    {
        let mut builder = Vec::with_capacity(left.len());
        for lhs in left {
            push_modulo_result::<L, R, Self, O>(lhs, other, &mut builder, ctx);
        }
        Value::Column(builder.into())
    }
}

macro_rules! impl_rem_scalar {
    ($t: ident, $strength_reduce: ident) => {
        impl ModuloValue for $t {}

        impl<L, R, O> RemScalar<L, R, O> for $t
        where
            L: Number + AsPrimitive<$t>,
            R: Number + AsPrimitive<$t>,
            $t: AsPrimitive<O>,
            O: Number,
        {
            fn rem_scalar<'a>(
                left: impl ExactSizeIterator<Item = &'a L>,
                other: &R,
                ctx: &mut EvalContext,
            ) -> Value<NumberType<O>>
            where
                L: 'a,
            {
                if *other == R::default() {
                    ctx.set_error(0, "Division by zero");
                    return Value::Column(vec![O::default(); left.len()].into());
                }
                let other: $t = other.as_();
                let reduced_rem = $strength_reduce::new(other);
                let iter = left.map(|v| (v.as_() % reduced_rem).as_());
                let col = NumberType::<O>::column_from_iter(iter, &[]);
                Value::Column(col)
            }
        }
    };
}

impl_rem_scalar!(u8, StrengthReducedU8);
impl_rem_scalar!(u16, StrengthReducedU16);
impl_rem_scalar!(u32, StrengthReducedU32);
impl_rem_scalar!(u64, StrengthReducedU64);

impl<L: Number, R: Number, O: Number> RemScalar<L, R, O> for i8 {}
impl<L: Number, R: Number, O: Number> RemScalar<L, R, O> for i16 {}
impl<L: Number, R: Number, O: Number> RemScalar<L, R, O> for i32 {}
impl<L: Number, R: Number, O: Number> RemScalar<L, R, O> for i64 {}
impl<L: Number, R: Number, O: Number> RemScalar<L, R, O> for F32 {}
impl<L: Number, R: Number, O: Number> RemScalar<L, R, O> for F64 {}

macro_rules! impl_default_modulo_value {
    ($($t: ident),*) => {
        $(impl ModuloValue for $t {})*
    };
}

impl_default_modulo_value!(F32, F64);

macro_rules! impl_signed_modulo_value {
    ($($t: ident),*) => {
        $(impl ModuloValue for $t {
            fn is_signed_min_modulo_minus_one(lhs: Self, rhs: Self) -> bool {
                lhs == <$t>::MIN && rhs == -1
            }
        })*
    };
}

impl_signed_modulo_value!(i8, i16, i32, i64);
