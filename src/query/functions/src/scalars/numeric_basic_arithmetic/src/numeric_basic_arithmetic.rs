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
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Scalar;
use databend_common_expression::arithmetics_type::ResultTypeOfBinary;
use databend_common_expression::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::stat_distribution::ArgStat;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::OwnedDistribution;
use databend_common_expression::stat_distribution::ReturnStat;
use databend_common_expression::stat_distribution::StatBinaryArg;
use databend_common_expression::types::ALL_FLOAT_TYPES;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::F64;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_float_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use num_traits::AsPrimitive;

use crate::arithmetic_modulo::ModuloValue;
use crate::arithmetic_modulo::RemScalar;
use crate::arithmetic_modulo::vectorize_modulo;

type AddMulResult<L, R> = <(L, R) as ResultTypeOfBinary>::AddMul;
type MinusResult<L, R> = <(L, R) as ResultTypeOfBinary>::Minus;
type IntDivResult<L, R> = <(L, R) as ResultTypeOfBinary>::IntDiv;
type ModuloResult<L, R> = <(L, R) as ResultTypeOfBinary>::Modulo;
type LeastSuperResult<L, R> = <(L, R) as ResultTypeOfBinary>::LeastSuper;

fn modulo_domain_with_cast<L, R, M, O>(
    lhs: &SimpleDomain<L>,
    rhs: &SimpleDomain<R>,
) -> FunctionDomain<NumberType<O>>
where
    L: Number,
    R: Number,
    M: Number,
    O: Number,
{
    if rhs.min <= R::default() && rhs.max >= R::default() {
        return FunctionDomain::MayThrow;
    }

    if M::FLOATING || O::FLOATING {
        return FunctionDomain::Full;
    }

    let (Some(lhs_min), Some(lhs_max), Some(rhs_min), Some(rhs_max)): (
        Option<M>,
        Option<M>,
        Option<M>,
        Option<M>,
    ) = (
        num_traits::cast::cast(lhs.min),
        num_traits::cast::cast(lhs.max),
        num_traits::cast::cast(rhs.min),
        num_traits::cast::cast(rhs.max),
    ) else {
        // Mixed signed/unsigned overloads evaluate through `as` casts. If a
        // range endpoint cannot be losslessly cast to the calculation type, the
        // wrapped value range is not represented by the original endpoints.
        return FunctionDomain::Full;
    };

    let Some(lhs_min) = number_to_i128(lhs_min) else {
        return FunctionDomain::Full;
    };
    let Some(lhs_max) = number_to_i128(lhs_max) else {
        return FunctionDomain::Full;
    };
    let Some(rhs_min) = number_to_i128(rhs_min) else {
        return FunctionDomain::Full;
    };
    let Some(rhs_max) = number_to_i128(rhs_max) else {
        return FunctionDomain::Full;
    };

    // Compute abs through unsigned space so signed minimum values widen safely.
    let max_abs_rhs = rhs_min.unsigned_abs().max(rhs_max.unsigned_abs());
    let Some(max_abs_remainder) = max_abs_rhs
        .checked_sub(1)
        .and_then(|value| i128::try_from(value).ok())
    else {
        return FunctionDomain::Full;
    };
    // The remainder keeps the dividend sign and cannot exceed either bound.
    let min = lhs_min.min(0).max(-max_abs_remainder);
    let max = lhs_max.max(0).min(max_abs_remainder);

    let (Some(min), Some(max)): (Option<O>, Option<O>) =
        (num_traits::cast::cast(min), num_traits::cast::cast(max))
    else {
        return FunctionDomain::Full;
    };

    FunctionDomain::Domain(SimpleDomain { min, max })
}

fn number_to_i128<N: Number>(value: N) -> Option<i128> {
    let scalar = N::upcast_scalar(value);
    match scalar {
        NumberScalar::UInt8(value) => Some(value.into()),
        NumberScalar::UInt16(value) => Some(value.into()),
        NumberScalar::UInt32(value) => Some(value.into()),
        NumberScalar::UInt64(value) => Some(value.into()),
        NumberScalar::Int8(value) => Some(value.into()),
        NumberScalar::Int16(value) => Some(value.into()),
        NumberScalar::Int32(value) => Some(value.into()),
        NumberScalar::Int64(value) => Some(value.into()),
        NumberScalar::Float32(_) | NumberScalar::Float64(_) => None,
    }
}

fn number_is_nan<N: Number>(value: N) -> bool {
    match N::upcast_scalar(value) {
        NumberScalar::Float32(value) => value.is_nan(),
        NumberScalar::Float64(value) => value.is_nan(),
        _ => false,
    }
}

fn domain_or_full<N: Number>(min: N, max: N) -> FunctionDomain<NumberType<N>> {
    if number_is_nan(min) || number_is_nan(max) {
        FunctionDomain::Full
    } else {
        FunctionDomain::Domain(SimpleDomain { min, max })
    }
}

fn derive_modulo_stat<L, R, M, O>(stat: StatBinaryArg) -> Result<Option<ReturnStat>, String>
where
    L: Number,
    R: Number,
    M: Number,
    O: Number,
{
    let Some(rhs) = stat.args[1].domain.as_singleton() else {
        return Ok(None);
    };
    if rhs.is_null() {
        return Ok(None);
    }

    derive_modulo_with_const::<L, R, M, O>(&rhs, &stat.args[0])
}

fn derive_modulo_with_const<L, R, M, O>(
    rhs: &Scalar,
    lhs: &ArgStat,
) -> Result<Option<ReturnStat>, String>
where
    L: Number,
    R: Number,
    M: Number,
    O: Number,
{
    if M::FLOATING || R::FLOATING || O::FLOATING {
        return Ok(None);
    }

    let rhs = NumberType::<R>::try_downcast_scalar(&rhs.as_ref()).map_err(|e| e.to_string())?;
    if rhs == R::default() {
        return Ok(None);
    }

    if let Ok(NullableDomain { has_null, value }) =
        NullableType::<NumberType<L>>::try_downcast_domain(&lhs.domain)
    {
        return Ok(Some(match value {
            Some(domain) => {
                let Some(domain) = modulo_domain_with_cast::<L, R, M, O>(&domain, &SimpleDomain {
                    min: rhs,
                    max: rhs,
                })
                .normalize() else {
                    return Ok(None);
                };
                // A signed modulo range can contain both signs, so bound NDV by the
                // derived output domain rather than by abs(divisor).
                let Some(ndv_upper) =
                    NumberType::<O>::upcast_domain(domain).finite_cardinality_upper()
                else {
                    return Ok(None);
                };
                ReturnStat {
                    domain: NullableType::<NumberType<O>>::upcast_domain(NullableDomain {
                        has_null,
                        value: Some(Box::new(domain)),
                    }),
                    ndv: lhs.ndv.reduce(ndv_upper as f64),
                    null_count: lhs.null_count,
                    distribution: OwnedDistribution::Unknown,
                }
            }
            None => ReturnStat {
                domain: NullableType::<NumberType<O>>::upcast_domain(NullableDomain {
                    has_null: true,
                    value: None,
                }),
                ndv: NdvEstimate::exact(0.0),
                null_count: lhs.null_count,
                distribution: OwnedDistribution::Unknown,
            },
        }));
    }

    let domain = NumberType::<L>::try_downcast_domain(&lhs.domain).map_err(|e| e.to_string())?;
    let Some(domain) =
        modulo_domain_with_cast::<L, R, M, O>(&domain, &SimpleDomain { min: rhs, max: rhs })
            .normalize()
    else {
        return Ok(None);
    };
    let output_domain = NumberType::<O>::upcast_domain(domain);
    // A signed modulo range can contain both signs, so bound NDV by the
    // derived output domain rather than by abs(divisor).
    let Some(ndv_upper) = output_domain.finite_cardinality_upper() else {
        return Ok(None);
    };

    Ok(Some(ReturnStat {
        domain: output_domain,
        ndv: lhs.ndv.reduce(ndv_upper as f64),
        null_count: lhs.null_count,
        distribution: OwnedDistribution::Unknown,
    }))
}

pub fn register_plus<L, R>(registry: &mut FunctionRegistry)
where
    L: Number + AsPrimitive<AddMulResult<L, R>>,
    R: Number + AsPrimitive<AddMulResult<L, R>>,
    (L, R): ResultTypeOfBinary,
    AddMulResult<L, R>: ResultTypeOfUnary + std::ops::Add<Output = AddMulResult<L, R>>,
{
    registry
        .scalar_builder("plus")
        .function()
        .typed_2_arg::<NumberType<L>, NumberType<R>, NumberType<AddMulResult<L, R>>>()
        .passthrough_nullable()
        .calc_domain(|_, lhs, rhs| {
            try {
                let lm: AddMulResult<L, R> = num_traits::cast::cast(lhs.max)?;
                let ln: AddMulResult<L, R> = num_traits::cast::cast(lhs.min)?;
                let rm: AddMulResult<L, R> = num_traits::cast::cast(rhs.max)?;
                let rn: AddMulResult<L, R> = num_traits::cast::cast(rhs.min)?;

                domain_or_full(ln.checked_add(rn)?, lm.checked_add(rm)?)
            }
            .unwrap_or(FunctionDomain::Full)
        })
        .derive_stat(|stat, _| {
            if let Some(value) = stat.args[0].domain.as_singleton() {
                return derive_plus_with_const::<L, R, AddMulResult<L, R>>(&value, &stat.args[1]);
            }
            if let Some(value) = stat.args[1].domain.as_singleton() {
                return derive_plus_with_const::<R, L, AddMulResult<L, R>>(&value, &stat.args[0]);
            }
            Ok(None)
        })
        .each_row(|a, b, _| {
            (AsPrimitive::<AddMulResult<L, R>>::as_(a))
                + (AsPrimitive::<AddMulResult<L, R>>::as_(b))
        })
        .register();
}

fn derive_plus_with_const<C, O, R>(
    cnst: &Scalar,
    stat: &ArgStat,
) -> Result<Option<ReturnStat>, String>
where
    C: Number + AsPrimitive<R>,
    O: Number + AsPrimitive<R>,
    R: Number + ResultTypeOfUnary + std::ops::Add<Output = R>,
{
    if cnst.is_null() {
        return Ok(None);
    }

    let cnst = NumberType::<C>::try_downcast_scalar(&cnst.as_ref())
        .map_err(|e| e.to_string())?
        .as_();

    if let Ok(NullableDomain { has_null, value }) =
        NullableType::<NumberType<O>>::try_downcast_domain(&stat.domain)
    {
        return Ok(try {
            ReturnStat {
                domain: NullableType::<NumberType<R>>::upcast_domain(NullableDomain {
                    has_null,
                    value: {
                        match value {
                            Some(domain) => Some(Box::new(SimpleDomain {
                                min: domain.min.as_().checked_add(cnst)?,
                                max: domain.max.as_().checked_add(cnst)?,
                            })),
                            None => None,
                        }
                    },
                }),
                ndv: stat.ndv,
                null_count: stat.null_count,
                distribution: OwnedDistribution::Unknown,
            }
        });
    }

    let domain = NumberType::<O>::try_downcast_domain(&stat.domain).map_err(|e| e.to_string())?;
    Ok(try {
        ReturnStat {
            domain: NumberType::<R>::upcast_domain(SimpleDomain {
                min: domain.min.as_().checked_add(cnst)?,
                max: domain.max.as_().checked_add(cnst)?,
            }),
            ndv: stat.ndv,
            null_count: stat.null_count,
            distribution: OwnedDistribution::Unknown,
        }
    })
}

pub fn register_minus<L, R>(registry: &mut FunctionRegistry)
where
    L: Number + AsPrimitive<MinusResult<L, R>>,
    R: Number + AsPrimitive<MinusResult<L, R>>,
    (L, R): ResultTypeOfBinary,
    MinusResult<L, R>: ResultTypeOfUnary + std::ops::Sub<Output = MinusResult<L, R>>,
{
    registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<MinusResult<L, R>>, _>(
        "minus",
        |_, lhs, rhs| {
            (|| {
                let lm: MinusResult<L, R> = num_traits::cast::cast(lhs.max)?;
                let ln: MinusResult<L, R> = num_traits::cast::cast(lhs.min)?;
                let rm: MinusResult<L, R> = num_traits::cast::cast(rhs.max)?;
                let rn: MinusResult<L, R> = num_traits::cast::cast(rhs.min)?;

                Some(domain_or_full(ln.checked_sub(rm)?, lm.checked_sub(rn)?))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| {
            (AsPrimitive::<MinusResult<L, R>>::as_(a)) - (AsPrimitive::<MinusResult<L, R>>::as_(b))
        },
    );
}

pub fn register_multiply<L, R>(registry: &mut FunctionRegistry)
where
    L: Number + AsPrimitive<AddMulResult<L, R>>,
    R: Number + AsPrimitive<AddMulResult<L, R>>,
    (L, R): ResultTypeOfBinary,
    AddMulResult<L, R>: ResultTypeOfUnary + std::ops::Mul<Output = AddMulResult<L, R>>,
{
    registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<AddMulResult<L, R>>, _>(
        "multiply",
        |_, lhs, rhs| {
            (|| {
                let lm: AddMulResult<L, R> = num_traits::cast::cast(lhs.max)?;
                let ln: AddMulResult<L, R> = num_traits::cast::cast(lhs.min)?;
                let rm: AddMulResult<L, R> = num_traits::cast::cast(rhs.max)?;
                let rn: AddMulResult<L, R> = num_traits::cast::cast(rhs.min)?;

                let x = lm.checked_mul(rm)?;
                let y = lm.checked_mul(rn)?;
                let m = ln.checked_mul(rm)?;
                let n = ln.checked_mul(rn)?;

                Some(domain_or_full(
                    x.min(y).min(m).min(n),
                    x.max(y).max(m).max(n),
                ))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| {
            (AsPrimitive::<AddMulResult<L, R>>::as_(a))
                * (AsPrimitive::<AddMulResult<L, R>>::as_(b))
        },
    );
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

pub fn register_divide<L, R>(registry: &mut FunctionRegistry)
where
    L: Number + AsPrimitive<F64>,
    R: Number + AsPrimitive<F64>,
{
    type T = F64;
    registry
        .register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "divide",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>>(
                |a, b, output, ctx| divide_function(a, b, output, ctx),
            ),
        );
}

pub fn div0_function<L: AsPrimitive<F64>, R: AsPrimitive<F64>>(a: L, b: R, output: &mut Vec<F64>) {
    let b: F64 = b.as_();
    if std::intrinsics::unlikely(b == 0.0) {
        output.push(F64::default()); // Push the default value for type T
    } else {
        output.push(AsPrimitive::<F64>::as_(a) / b);
    }
}

pub fn divnull_function<L: AsPrimitive<F64>, R: AsPrimitive<F64>>(a: L, b: R) -> Option<F64> {
    let b: F64 = b.as_();
    if std::intrinsics::unlikely(b == 0.0) {
        None
    } else {
        Some(AsPrimitive::<F64>::as_(a) / b)
    }
}

pub fn register_intdiv<L, R>(registry: &mut FunctionRegistry)
where
    L: Number + AsPrimitive<f64>,
    R: Number + AsPrimitive<F64>,
    (L, R): ResultTypeOfBinary,
    F64: AsPrimitive<IntDivResult<L, R>>,
{
    registry.register_passthrough_nullable_2_arg::<
        NumberType<L>,
        NumberType<R>,
        NumberType<IntDivResult<L, R>>,
        _,
        _,
    >(
        "div",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<
            NumberType<L>,
            NumberType<R>,
            NumberType<IntDivResult<L, R>>,
        >(|a, b, output, ctx| {
            let b_value: F64 = b.as_();
            if std::intrinsics::unlikely(b_value == 0.0) {
                ctx.set_error(output.len(), "divided by zero");
                output.push(IntDivResult::<L, R>::default());
            } else {
                let lhs = F64::from(AsPrimitive::<f64>::as_(a));
                output.push(AsPrimitive::<IntDivResult<L, R>>::as_(lhs / b_value));
            }
        }),
    );
}

pub fn register_modulo<L, R>(registry: &mut FunctionRegistry)
where
    L: Number + AsPrimitive<LeastSuperResult<L, R>>,
    R: Number + AsPrimitive<LeastSuperResult<L, R>> + AsPrimitive<F64> + AsPrimitive<f64>,
    (L, R): ResultTypeOfBinary,
    LeastSuperResult<L, R>: Number
        + AsPrimitive<ModuloResult<L, R>>
        + Rem<Output = LeastSuperResult<L, R>>
        + RemScalar<L, R, ModuloResult<L, R>>
        + ModuloValue,
    ModuloResult<L, R>: Number,
{
    registry
        .scalar_builder("modulo")
        .function()
        .typed_2_arg::<NumberType<L>, NumberType<R>, NumberType<ModuloResult<L, R>>>()
        .passthrough_nullable()
        .calc_domain(|_, lhs, rhs| {
            modulo_domain_with_cast::<L, R, LeastSuperResult<L, R>, ModuloResult<L, R>>(lhs, rhs)
        })
        .derive_stat(|stat, _| {
            derive_modulo_stat::<L, R, LeastSuperResult<L, R>, ModuloResult<L, R>>(stat)
        })
        .vectorized(vectorize_modulo::<
            L,
            R,
            LeastSuperResult<L, R>,
            ModuloResult<L, R>,
        >())
        .register();
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
                        register_plus::<L, R>(registry);
                        register_minus::<L, R>(registry);
                        register_multiply::<L, R>(registry);
                        register_divide::<L, R>(registry);
                        register_intdiv::<L, R>(registry);
                        register_modulo::<L, R>(registry);
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
                        register_plus::<L, R>(registry);
                        register_minus::<L, R>(registry);
                        register_multiply::<L, R>(registry);
                        register_divide::<L, R>(registry);
                        register_intdiv::<L, R>(registry);
                        register_modulo::<L, R>(registry);
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
                        register_plus::<L, R>(registry);
                        register_minus::<L, R>(registry);
                        register_multiply::<L, R>(registry);
                        register_divide::<L, R>(registry);
                        register_intdiv::<L, R>(registry);
                        register_modulo::<L, R>(registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }
}
