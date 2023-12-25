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

use databend_common_expression::types::number::*;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ValueType;
use databend_common_expression::EvalContext;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use num_traits::AsPrimitive;
use strength_reduce::StrengthReducedU16;
use strength_reduce::StrengthReducedU32;
use strength_reduce::StrengthReducedU64;
use strength_reduce::StrengthReducedU8;

pub(crate) fn vectorize_modulo<L, R, M, O>()
-> impl Fn(ValueRef<NumberType<L>>, ValueRef<NumberType<R>>, &mut EvalContext) -> Value<NumberType<O>>
+ Copy
where
    L: Number + AsPrimitive<M>,
    R: Number + AsPrimitive<M> + AsPrimitive<f64>,
    M: Number + AsPrimitive<O> + Rem<Output = M> + RemScalar<O>,
    O: Number,
{
    move |arg1, arg2, ctx| {
        let apply = |lhs: &L, rhs: &R, builder: &mut Vec<O>, ctx: &mut EvalContext| {
            let r: f64 = rhs.as_();
            if r == 0.0 {
                ctx.set_error(builder.len(), "Division by zero");
                builder.push(O::default());
            } else {
                builder.push((lhs.as_() % rhs.as_()).as_());
            }
        };

        match (arg1, arg2) {
            (ValueRef::Column(lhs), ValueRef::Scalar(rhs)) => {
                if rhs == R::default() {
                    ctx.set_error(0, "Division by zero");
                    return Value::Column(vec![O::default(); lhs.len()].into());
                }
                let iter = lhs.iter().map(|lhs| lhs.as_());
                RemScalar::<O>::rem_scalar(iter, rhs.as_())
            }
            (ValueRef::Scalar(lhs), ValueRef::Scalar(rhs)) => {
                let mut builder: Vec<O> = Vec::with_capacity(1);
                apply(&lhs, &rhs, &mut builder, ctx);
                Value::Scalar(NumberType::<O>::build_scalar(builder))
            }
            (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
                let mut builder: Vec<O> = Vec::with_capacity(arg2.len());
                for val in arg2.iter() {
                    apply(&arg1, val, &mut builder, ctx);
                }
                Value::Column(builder.into())
            }
            (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
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

pub trait RemScalar<O: Number>: Number {
    fn rem_scalar(_left: impl Iterator<Item = Self>, _other: Self) -> Value<NumberType<O>> {
        unimplemented!()
    }
}

macro_rules! impl_rem_scalar {
    ($t: ident, $strength_reduce: ident) => {
        impl<O> RemScalar<O> for $t
        where
            Self: AsPrimitive<O> + AsPrimitive<f64> + Rem<Output = Self>,
            O: Number,
        {
            fn rem_scalar(left: impl Iterator<Item = Self>, other: Self) -> Value<NumberType<O>> {
                let reduced_rem = $strength_reduce::new(other);
                let iter = left.map(|v| (v % reduced_rem).as_());
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

impl<O: Number> RemScalar<O> for i8 {}
impl<O: Number> RemScalar<O> for i16 {}
impl<O: Number> RemScalar<O> for i32 {}
impl<O: Number> RemScalar<O> for i64 {}
impl<O: Number> RemScalar<O> for F32 {}
impl<O: Number> RemScalar<O> for F64 {}
