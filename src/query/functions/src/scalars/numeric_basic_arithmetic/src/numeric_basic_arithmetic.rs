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

use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::arithmetics_type::ResultTypeOfBinary;
use databend_common_expression::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::types::ALL_FLOAT_TYPES;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::F64;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_float_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use num_traits::AsPrimitive;

use crate::arithmetic_modulo::vectorize_modulo;

#[macro_export]
macro_rules! register_plus {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::AddMul;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "plus",
            |_, lhs, rhs| {
                (|| {
                    let lm: T = num_traits::cast::cast(lhs.max)?;
                    let ln: T = num_traits::cast::cast(lhs.min)?;
                    let rm: T = num_traits::cast::cast(rhs.max)?;
                    let rn: T = num_traits::cast::cast(rhs.min)?;

                    Some(FunctionDomain::Domain(SimpleDomain::<T> {
                        min: ln.checked_add(rn)?,
                        max: lm.checked_add(rm)?,
                    }))
                })()
                .unwrap_or(FunctionDomain::Full)
            },
            |a, b, _| (AsPrimitive::<T>::as_(a)) + (AsPrimitive::<T>::as_(b)),
        );
    };
}

#[macro_export]
macro_rules! register_minus {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::Minus;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "minus",
            |_, lhs, rhs| {
                (|| {
                    let lm: T = num_traits::cast::cast(lhs.max)?;
                    let ln: T = num_traits::cast::cast(lhs.min)?;
                    let rm: T = num_traits::cast::cast(rhs.max)?;
                    let rn: T = num_traits::cast::cast(rhs.min)?;

                    Some(FunctionDomain::Domain(SimpleDomain::<T> {
                        min: ln.checked_sub(rm)?,
                        max: lm.checked_sub(rn)?,
                    }))
                })()
                .unwrap_or(FunctionDomain::Full)
            },
            |a, b, _| (AsPrimitive::<T>::as_(a)) - (AsPrimitive::<T>::as_(b)),
        );
    };
}

#[macro_export]
macro_rules! register_multiply {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::AddMul;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "multiply",
            |_, lhs, rhs| {
                (|| {
                    let lm: T = num_traits::cast::cast(lhs.max)?;
                    let ln: T = num_traits::cast::cast(lhs.min)?;
                    let rm: T = num_traits::cast::cast(rhs.max)?;
                    let rn: T = num_traits::cast::cast(rhs.min)?;

                    let x = lm.checked_mul(rm)?;
                    let y = lm.checked_mul(rn)?;
                    let m = ln.checked_mul(rm)?;
                    let n = ln.checked_mul(rn)?;

                    Some(FunctionDomain::Domain(SimpleDomain::<T> {
                        min: x.min(y).min(m).min(n),
                        max: x.max(y).max(m).max(n),
                    }))
                })()
                .unwrap_or(FunctionDomain::Full)
            },
            |a, b, _| (AsPrimitive::<T>::as_(a)) * (AsPrimitive::<T>::as_(b)),
        );
    };
}

pub fn divide_function<L: AsPrimitive<F64>, R: AsPrimitive<F64>>(
    a: L,
    b: R,
    output: &mut Vec<F64>,
    ctx: &mut EvalContext,
) {
    let b: F64 = b.as_();
    if std::intrinsics::unlikely(b == 0.0) {
        ctx.set_error(output.len(), "divided by zero");
        output.push(F64::default());
    } else {
        output.push((AsPrimitive::<F64>::as_(a)) / b);
    }
}

#[macro_export]
macro_rules! register_divide {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = F64;
         $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "divide",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                |a, b, output, ctx| divide_function(a, b, output, ctx)
           ),
        );
    };
}

pub fn div0_function<L: AsPrimitive<F64>, R: AsPrimitive<F64>>(a: L, b: R, output: &mut Vec<F64>) {
    let b: F64 = b.as_();
    if std::intrinsics::unlikely(b == 0.0) {
        output.push(F64::default()); // Push the default value for type T
    } else {
        output.push(AsPrimitive::<F64>::as_(a) / b);
    }
}

#[macro_export]
macro_rules! register_div0 {
    ($lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = F64;

        $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "div0",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>>(
                |a, b, output, _ctx| div0_function(a, b, output)
            ),
        );
    };
}

pub fn divnull_function<L: AsPrimitive<F64>, R: AsPrimitive<F64>>(a: L, b: R) -> Option<F64> {
    let b: F64 = b.as_();
    if std::intrinsics::unlikely(b == 0.0) {
        None
    } else {
        Some(AsPrimitive::<F64>::as_(a) / b)
    }
}

#[macro_export]
macro_rules! register_divnull {
    ($lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = F64;

        $registry.register_2_arg_core::<NullableType<NumberType<L>>, NullableType<NumberType<R>>, NullableType<NumberType<T>>, _, _>(
            "divnull",
            |_, _, _| FunctionDomain::Full,
            vectorize_2_arg::<NullableType<NumberType<L>>, NullableType<NumberType<R>>, NullableType<NumberType<T>>>(|a, b, _| {
                match (a, b) {
                    (Some(a), Some(b)) => {
                        divnull_function(a,b)
                    },
                    _ => None,
                }
            }));
    }
}

#[macro_export]
macro_rules! register_intdiv {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::IntDiv;
        $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
            "div",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>>(
                |a, b, output, ctx| {
                    let b: F64 = b.as_();
                    if std::intrinsics::unlikely(b == 0.0) {
                        ctx.set_error(output.len(), "divided by zero");
                        output.push(T::default());
                    } else {
                        output.push(AsPrimitive::<T>::as_((F64::from(AsPrimitive::<f64>::as_(a))) / b));
                    }
                }
            ),
        );
    };
}

#[macro_export]
macro_rules! register_modulo {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type M = <(L, R) as ResultTypeOfBinary>::LeastSuper;
        type T = <(L, R) as ResultTypeOfBinary>::Modulo;

        let rtype = M::data_type();
        // slow path for modulo
        if !matches!(
            rtype,
            NumberDataType::UInt8
                | NumberDataType::UInt16
                | NumberDataType::UInt32
                | NumberDataType::UInt64
        ) {
            $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                "modulo",

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                    |a, b, output, ctx| {
                        let b: F64 = b.as_();
                        if std::intrinsics::unlikely(b == 0.0) {
                            ctx.set_error(output.len(), "divided by zero");
                            output.push(T::default());
                        } else {
                            output.push(AsPrimitive::<T>::as_((AsPrimitive::<M>::as_(a)) % (AsPrimitive::<M>::as_(b))));
                        }
                    }
                ),
            );
        } else {
            $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                "modulo",

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_modulo::<L, R, M, T>()
            );
        }
    };
}

#[macro_export]
macro_rules! register_basic_arithmetic {
    ( $lt:ty, $rt:ty, $registry:expr) => {{
        register_plus!($lt, $rt, $registry);
    }
    {
        register_minus!($lt, $rt, $registry);
    }
    {
        register_multiply!($lt, $rt, $registry);
    }
    {
        register_divide!($lt, $rt, $registry);
    }
    {
        register_intdiv!($lt, $rt, $registry);
    }
    {
        register_modulo!($lt, $rt, $registry);
    }};
}

pub fn register_div_arithmetic(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, _, _>(
        "div0",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>>(
            |a, b, output, _ctx| div0_function(a, b, output)
        ),
    );

    registry.register_2_arg_core::<NullableType<NumberType<F64>>, NullableType<NumberType<F64>>, NullableType<NumberType<F64>>, _, _>(
        "divnull",
        |_, _, _| FunctionDomain::Full,
        vectorize_2_arg::<NullableType<NumberType<F64>>, NullableType<NumberType<F64>>, NullableType<NumberType<F64>>>(|a, b, _| {
            match (a, b) {
                (Some(a), Some(b)) => {
                    divnull_function(a,b)
                },
                _ => None,
            }
        }));
}

pub fn register_numeric_basic_arithmetic(registry: &mut FunctionRegistry) {
    register_div_arithmetic(registry);

    for left in ALL_INTEGER_TYPES {
        for right in ALL_FLOAT_TYPES {
            with_integer_mapped_type!(|L| match left {
                NumberDataType::L => with_float_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    for left in ALL_FLOAT_TYPES {
        for right in ALL_INTEGER_TYPES {
            with_float_mapped_type!(|L| match left {
                NumberDataType::L => with_integer_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    for left in ALL_FLOAT_TYPES {
        for right in ALL_FLOAT_TYPES {
            with_float_mapped_type!(|L| match left {
                NumberDataType::L => with_float_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }
}
