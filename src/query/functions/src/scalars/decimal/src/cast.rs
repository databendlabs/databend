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

use std::cmp::Ordering;
use std::ops::Div;
use std::ops::Mul;
use std::sync::Arc;

use databend_common_base::base::OrderedFloat;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::compute_view::Compute;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::i256;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use num_traits::AsPrimitive;

use crate::cast_from_jsonb::variant_to_decimal;

// int float to decimal
pub fn register_to_decimal(registry: &mut FunctionRegistry) {
    let factory = |params: &[Scalar], args_type: &[DataType]| {
        if args_type.len() != 1 || params.len() != 2 {
            return None;
        }

        let from_type = args_type[0].remove_nullable();

        if !matches!(
            from_type,
            DataType::Boolean
                | DataType::Number(_)
                | DataType::Decimal(_)
                | DataType::String
                | DataType::Variant
        ) {
            return None;
        }

        let decimal_size =
            DecimalSize::new(params[0].get_i64()? as _, params[1].get_i64()? as _).ok()?;

        let return_type = if from_type == DataType::Variant {
            DataType::Nullable(Box::new(DataType::Decimal(decimal_size)))
        } else {
            DataType::Decimal(decimal_size)
        };
        Some(Function {
            signature: FunctionSignature {
                name: "to_decimal".to_string(),
                args_type: vec![from_type.clone()],
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |ctx, d| {
                    let decimal_type = DecimalDataType::from(decimal_size);
                    convert_to_decimal_domain(ctx, d[0].clone(), decimal_type)
                        .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                        .unwrap_or(FunctionDomain::MayThrow)
                }),
                eval: Box::new(move |args, ctx| {
                    let desc_type = if decimal_size.can_carried_by_64() && !ctx.strict_eval {
                        DecimalDataType::Decimal64(decimal_size)
                    } else {
                        DecimalDataType::from(decimal_size)
                    };
                    convert_to_decimal(&args[0], ctx, &from_type, desc_type)
                }),
            },
        })
    };

    registry.register_function_factory(
        "to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            Some(Arc::new(factory(params, args_type)?))
        })),
    );
    registry.register_function_factory(
        "to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let f = factory(params, args_type)?;
            Some(Arc::new(f.passthrough_nullable()))
        })),
    );
    registry.register_function_factory(
        "try_to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let mut f = factory(params, args_type)?;
            f.signature.name = "try_to_decimal".to_string();
            Some(Arc::new(f.error_to_null()))
        })),
    );
    registry.register_function_factory(
        "try_to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let mut f = factory(params, args_type)?;
            f.signature.name = "try_to_decimal".to_string();
            Some(Arc::new(f.error_to_null().passthrough_nullable()))
        })),
    );

    let as_decimal = FunctionFactory::Closure(Box::new(|params, args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant {
            return None;
        }
        let precision = if !params.is_empty() {
            params[0].get_i64()? as u8
        } else {
            MAX_DECIMAL128_PRECISION
        };
        let scale = if params.len() > 1 {
            params[1].get_i64()? as u8
        } else {
            0
        };
        let decimal_size = DecimalSize::new(precision, scale).ok()?;
        let decimal_type = DecimalDataType::from(decimal_size);

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "as_decimal".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Nullable(Box::new(DataType::Decimal(decimal_size))),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| convert_as_decimal(&args[0], ctx, decimal_type)),
            },
        }))
    }));
    registry.register_function_factory("as_decimal", as_decimal);
}

pub fn register_decimal_to_float<T: Number>(registry: &mut FunctionRegistry) {
    let data_type = NumberType::<T>::data_type();
    debug_assert!(data_type.is_floating());

    let is_f32 = matches!(data_type, DataType::Number(NumberDataType::Float32));

    let factory = |_params: &[Scalar], args_type: &[DataType], data_type: DataType| {
        if args_type.len() != 1 {
            return None;
        }

        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }
        let is_f32 = matches!(data_type, DataType::Number(NumberDataType::Float32));
        let name = if is_f32 { "to_float32" } else { "to_float64" };
        let calc_domain = if is_f32 {
            Box::new(|_: &_, d: &[Domain]| {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match d[0].as_decimal().unwrap() {
                    DecimalDomain::DECIMAL_TYPE(d, size) => {
                        FunctionDomain::Domain(Domain::Number(NumberDomain::Float32(
                            SimpleDomain {
                                min: OrderedFloat(d.min.to_float32(size.scale())),
                                max: OrderedFloat(d.max.to_float32(size.scale())),
                            },
                        )))
                    }
                })
            }) as _
        } else {
            Box::new(|_: &_, d: &[Domain]| {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match d[0].as_decimal().unwrap() {
                    DecimalDomain::DECIMAL_TYPE(d, size) => {
                        FunctionDomain::Domain(Domain::Number(NumberDomain::Float64(
                            SimpleDomain {
                                min: OrderedFloat(d.min.to_float64(size.scale())),
                                max: OrderedFloat(d.max.to_float64(size.scale())),
                            },
                        )))
                    }
                })
            }) as _
        };

        let eval = if is_f32 {
            Box::new(move |args: &[Value<AnyType>], tx: &mut EvalContext| {
                decimal_to_float::<F32>(&args[0], tx)
            }) as _
        } else {
            Box::new(move |args: &[Value<AnyType>], tx: &mut EvalContext| {
                decimal_to_float::<F64>(&args[0], tx)
            }) as _
        };

        let function = Function {
            signature: FunctionSignature {
                name: name.to_string(),
                args_type: vec![arg_type.clone()],
                return_type: data_type.clone(),
            },
            eval: FunctionEval::Scalar { calc_domain, eval },
        };

        Some(function)
    };

    let name = if is_f32 { "to_float32" } else { "to_float64" };

    registry.register_function_factory(
        name,
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let data_type = NumberType::<T>::data_type();
            Some(Arc::new(factory(params, args_type, data_type)?))
        })),
    );
    registry.register_function_factory(
        name,
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let data_type = NumberType::<T>::data_type();
            let f = factory(params, args_type, data_type)?;
            Some(Arc::new(f.passthrough_nullable()))
        })),
    );
    registry.register_function_factory(
        &format!("try_{name}"),
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let data_type = NumberType::<T>::data_type();
            let mut f = factory(params, args_type, data_type)?;
            f.signature.name = format!("try_{name}");
            Some(Arc::new(f.error_to_null()))
        })),
    );
    registry.register_function_factory(
        &format!("try_{name}"),
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let data_type = NumberType::<T>::data_type();
            let mut f = factory(params, args_type, data_type)?;
            f.signature.name = format!("try_{name}");
            Some(Arc::new(f.error_to_null().passthrough_nullable()))
        })),
    );
}

pub fn register_decimal_to_int<T: Number>(registry: &mut FunctionRegistry) {
    if T::data_type().is_float() {
        return;
    }
    let name = format!("to_{}", T::data_type().to_string().to_lowercase());
    let try_name = format!("try_to_{}", T::data_type().to_string().to_lowercase());

    let factory = |_params: &[Scalar], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }

        let name = format!("to_{}", T::data_type().to_string().to_lowercase());
        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }

        let function = Function {
            signature: FunctionSignature {
                name,
                args_type: vec![arg_type.clone()],
                return_type: DataType::Number(T::data_type()),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|ctx, d| {
                    let res_fn = move || match d[0].as_decimal().unwrap() {
                        DecimalDomain::Decimal64(d, size) => Some(SimpleDomain::<T> {
                            min: d.min.to_int(size.scale(), ctx.rounding_mode)?,
                            max: d.max.to_int(size.scale(), ctx.rounding_mode)?,
                        }),
                        DecimalDomain::Decimal128(d, size) => Some(SimpleDomain::<T> {
                            min: d.min.to_int(size.scale(), ctx.rounding_mode)?,
                            max: d.max.to_int(size.scale(), ctx.rounding_mode)?,
                        }),
                        DecimalDomain::Decimal256(d, size) => Some(SimpleDomain::<T> {
                            min: d.min.to_int(size.scale(), ctx.rounding_mode)?,
                            max: d.max.to_int(size.scale(), ctx.rounding_mode)?,
                        }),
                    };

                    res_fn()
                        .map(|d| FunctionDomain::Domain(Domain::Number(T::upcast_domain(d))))
                        .unwrap_or(FunctionDomain::MayThrow)
                }),
                eval: Box::new(move |args, ctx| decimal_to_int::<T>(&args[0], ctx)),
            },
        };

        Some(function)
    };

    registry.register_function_factory(
        &name,
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            Some(Arc::new(factory(params, args_type)?))
        })),
    );
    registry.register_function_factory(
        &name,
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let f = factory(params, args_type)?;
            Some(Arc::new(f.passthrough_nullable()))
        })),
    );
    registry.register_function_factory(
        &try_name,
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let mut f = factory(params, args_type)?;
            f.signature.name = format!("try_to_{}", T::data_type().to_string().to_lowercase());
            Some(Arc::new(f.error_to_null()))
        })),
    );
    registry.register_function_factory(
        &try_name,
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let mut f = factory(params, args_type)?;
            f.signature.name = format!("try_to_{}", T::data_type().to_string().to_lowercase());
            Some(Arc::new(f.error_to_null().passthrough_nullable()))
        })),
    );
}

pub fn register_decimal_to_string(registry: &mut FunctionRegistry) {
    // decimal to string
    let factory = |_params: &[Scalar], args_type: &[DataType]| {
        let (size, nullable) = match args_type {
            [DataType::Nullable(box DataType::Decimal(size))] => (*size, true),
            [DataType::Decimal(size)] => (*size, false),
            _ => return None,
        };

        let function = Function {
            signature: FunctionSignature {
                name: "to_string".to_string(),
                args_type: vec![DataType::Decimal(size)],
                return_type: StringType::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| {
                    if let Some(arg) = args[0].try_downcast::<Decimal128Type>() {
                        let arg_type = DecimalDataType::Decimal128(size);
                        return decimal_to_string(arg, arg_type, ctx).upcast();
                    };
                    if let Some(arg) = args[0].try_downcast::<Decimal256Type>() {
                        let arg_type = DecimalDataType::Decimal256(size);
                        return decimal_to_string(arg, arg_type, ctx).upcast();
                    };
                    unreachable!()
                }),
            },
        };

        if nullable {
            Some(Arc::new(function.passthrough_nullable()))
        } else {
            Some(Arc::new(function))
        }
    };
    registry.register_function_factory("to_string", FunctionFactory::Closure(Box::new(factory)));
}

fn decimal_to_string<T: Decimal>(
    arg: Value<DecimalType<T>>,
    from_type: DecimalDataType,
    ctx: &mut EvalContext,
) -> Value<StringType> {
    let scale = from_type.scale();
    vectorize_1_arg::<DecimalType<T>, StringType>(|v, _| v.to_decimal_string(scale))(arg, ctx)
}

pub fn convert_to_decimal(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    if from_type.is_decimal() {
        let (from_type, _) = DecimalDataType::from_value(arg).unwrap();
        decimal_to_decimal(arg, ctx, from_type, dest_type)
    } else {
        other_to_decimal(arg, ctx, from_type, dest_type)
    }
}

pub fn other_to_decimal(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    if let Some(v) = try {
        let size = dest_type.as_decimal64()?;
        if size.scale() != 0 || size.precision() < 19 {
            None?
        }
        let buffer = arg.as_column()?.as_number()?.as_int64()?;
        Value::<Decimal64Type>::Column(buffer.clone()).upcast_decimal(*size)
    } {
        return v;
    }

    with_decimal_mapped_type!(|DECIMAL_TYPE| match dest_type {
        DecimalDataType::DECIMAL_TYPE(_) => {
            other_to_decimal_type::<DECIMAL_TYPE>(arg, ctx, from_type, dest_type)
        }
    })
}

fn other_to_decimal_type<T>(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType>
where
    T: Decimal + Mul<Output = T> + Div<Output = T>,
{
    let size = dest_type.size();
    match from_type {
        DataType::Boolean => {
            let arg = arg.try_downcast().unwrap();
            vectorize_1_arg::<BooleanType, DecimalType<T>>(|a: bool, _| {
                if a {
                    T::e(size.scale())
                } else {
                    T::zero()
                }
            })(arg, ctx)
        }

        DataType::Number(ty) => {
            if ty.is_float() {
                match ty {
                    NumberDataType::Float32 => {
                        let arg = arg.try_downcast().unwrap();
                        float_to_decimal::<T, NumberType<F32>>(arg, ctx, size)
                    }
                    NumberDataType::Float64 => {
                        let arg = arg.try_downcast().unwrap();
                        float_to_decimal::<T, NumberType<F64>>(arg, ctx, size)
                    }
                    _ => unreachable!(),
                }
            } else {
                with_integer_mapped_type!(|NUM_TYPE| match ty {
                    NumberDataType::NUM_TYPE => {
                        let arg = arg.try_downcast().unwrap();
                        integer_to_decimal::<T, NumberType<NUM_TYPE>>(arg, ctx, size)
                    }
                    _ => unreachable!(),
                })
            }
        }
        DataType::String => {
            let arg = arg.try_downcast().unwrap();
            string_to_decimal::<T>(arg, ctx, size)
        }
        DataType::Variant => {
            let arg = arg.try_downcast().unwrap();
            let result = variant_to_decimal::<T>(arg, ctx, dest_type, false);
            return result.upcast_decimal(size);
        }
        _ => unreachable!("to_decimal not support this DataType"),
    }
    .upcast_decimal(size)
}

fn convert_as_decimal(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let size = dest_type.size();
    with_decimal_mapped_type!(|DECIMAL_TYPE| match dest_type {
        DecimalDataType::DECIMAL_TYPE(_) => {
            let arg = arg.try_downcast().unwrap();
            let result = variant_to_decimal::<DECIMAL_TYPE>(arg, ctx, dest_type, true);
            result.upcast_decimal(size)
        }
    })
}

pub fn convert_to_decimal_domain(
    func_ctx: &FunctionContext,
    domain: Domain,
    dest_type: DecimalDataType,
) -> Option<DecimalDomain> {
    // Convert the domain to a Column.
    // The first row is the min value, the second row is the max value.
    let column = match domain {
        Domain::Number(number_domain) => {
            with_number_mapped_type!(|NUM_TYPE| match number_domain {
                NumberDomain::NUM_TYPE(d) => {
                    let min = d.min;
                    let max = d.max;
                    NumberType::<NUM_TYPE>::from_data(vec![min, max])
                }
            })
        }
        Domain::Boolean(d) => {
            let min = !d.has_false;
            let max = d.has_true;
            BooleanType::from_data(vec![min, max])
        }
        Domain::Decimal(d) => {
            with_decimal_mapped_type!(|DECIMAL| match d {
                DecimalDomain::DECIMAL(d, size) => {
                    let min = d.min;
                    let max = d.max;
                    DecimalType::from_data_with_size(vec![min, max], Some(size))
                }
            })
        }
        Domain::String(d) => {
            let min = d.min;
            let max = d.max?;
            StringType::from_data(vec![min, max])
        }
        _ => {
            return None;
        }
    };

    let from_type = column.data_type();
    let value = Value::<AnyType>::Column(column);
    let mut ctx = EvalContext {
        generics: &[],
        num_rows: 2,
        func_ctx,
        validity: None,
        errors: None,
        suppress_error: false,
        strict_eval: true,
    };
    let dest_size = dest_type.size();
    let res = convert_to_decimal(&value, &mut ctx, &from_type, dest_type);

    if ctx.errors.is_some() {
        return None;
    }
    let decimal_col = res.as_column()?.as_decimal()?;
    assert_eq!(decimal_col.len(), 2);

    let domain = with_decimal_type!(|DECIMAL| match decimal_col {
        DecimalColumn::DECIMAL(buf, size) => {
            assert_eq!(&dest_size, size);
            let (min, max) = unsafe { (*buf.get_unchecked(0), *buf.get_unchecked(1)) };
            DecimalDomain::DECIMAL(SimpleDomain { min, max }, *size)
        }
    });
    Some(domain)
}

fn string_to_decimal<T>(
    from: Value<StringType>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> Value<DecimalType<T>>
where
    T: Decimal + Mul<Output = T>,
{
    let f = |x: &str, builder: &mut Vec<T>, ctx: &mut EvalContext| {
        let value =
            match read_decimal_with_size::<T>(x.as_bytes(), size, true, ctx.func_ctx.rounding_mode)
            {
                Ok((d, _)) => d,
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    T::zero()
                }
            };

        builder.push(value);
    };

    vectorize_with_builder_1_arg::<StringType, DecimalType<T>>(f)(from, ctx)
}

fn integer_to_decimal<T, S>(
    from: Value<S>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> Value<DecimalType<T>>
where
    T: Decimal + Mul<Output = T>,
    S: AccessType,
    for<'a> S::ScalarRef<'a>: Number + AsPrimitive<i128>,
{
    let multiplier = T::e(size.scale());

    let min_for_precision = T::min_for_precision(size.precision());
    let max_for_precision = T::max_for_precision(size.precision());

    let never_overflow = if size.scale() == 0 {
        true
    } else {
        [
            <S::ScalarRef<'_> as Number>::MIN,
            <S::ScalarRef<'_> as Number>::MAX,
        ]
        .into_iter()
        .all(|x| {
            let Some(x) = T::from_i128(x.as_()).checked_mul(multiplier) else {
                return false;
            };
            x >= min_for_precision && x <= max_for_precision
        })
    };

    if never_overflow {
        vectorize_1_arg(|x: S::ScalarRef<'_>, _| T::from_i128(x.as_()) * multiplier)(from, ctx)
    } else {
        let f = |x: S::ScalarRef<'_>, builder: &mut Vec<T>, ctx: &mut EvalContext| {
            if let Some(x) = T::from_i128(x.as_()).checked_mul(multiplier) {
                if x > max_for_precision || x < min_for_precision {
                    ctx.set_error(
                        builder.len(),
                        concat!("Decimal overflow at line : ", line!()),
                    );
                    builder.push(T::one());
                } else {
                    builder.push(x);
                }
            } else {
                ctx.set_error(
                    builder.len(),
                    concat!("Decimal overflow at line : ", line!()),
                );
                builder.push(T::one());
            }
        };
        vectorize_with_builder_1_arg(f)(from, ctx)
    }
}

fn float_to_decimal<T: Decimal, S: AccessType>(
    from: Value<S>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> Value<DecimalType<T>>
where
    for<'a> S::ScalarRef<'a>: Number + AsPrimitive<f64>,
{
    let multiplier: f64 = (10_f64).powi(size.scale() as i32).as_();

    let min_for_precision = T::min_for_precision(size.precision());
    let max_for_precision = T::max_for_precision(size.precision());

    let f = |x: S::ScalarRef<'_>, builder: &mut Vec<T>, ctx: &mut EvalContext| {
        let mut x = x.as_() * multiplier;
        if ctx.func_ctx.rounding_mode {
            x = x.round();
        }
        let x = T::from_float(x);
        if x > max_for_precision || x < min_for_precision {
            ctx.set_error(
                builder.len(),
                concat!("Decimal overflow at line : ", line!()),
            );
            builder.push(T::one());
        } else {
            builder.push(x);
        }
    };

    vectorize_with_builder_1_arg(f)(from, ctx)
}

#[inline]
pub(super) fn get_round_val<T: Decimal>(x: T, scale: u8, rounding_mode: bool) -> Option<T> {
    if !rounding_mode || scale == 0 {
        return None;
    }
    // Checking whether numbers need to be added or subtracted to calculate rounding
    let q = x.checked_div(T::e(scale - 1))?;
    let m = q.checked_rem(T::from_i128(10))?;
    if m >= T::from_i128(5) {
        return Some(T::one());
    }
    if m <= T::from_i128(-5) {
        return Some(T::minus_one());
    }
    None
}

fn decimal_shrink_cast<F, T, C>(
    from_size: DecimalSize,
    dest_size: DecimalSize,
    buffer: Value<DecimalType<F>>,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    F: Decimal + Copy,
    T: Decimal + Copy,
    C: Compute<CoreDecimal<F>, CoreDecimal<T>>,
{
    let max = F::max_for_precision(dest_size.precision());
    let min = F::min_for_precision(dest_size.precision());

    match dest_size.scale().cmp(&from_size.scale()) {
        Ordering::Equal => vectorize_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
            |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| {
                if x <= max && x >= min {
                    builder.push(C::compute(&x));
                } else {
                    ctx.set_error(
                        builder.len(),
                        concat!("Decimal overflow at line : ", line!()),
                    );
                    builder.push(T::one());
                }
            },
        )(buffer, ctx),
        Ordering::Greater => {
            let factor = F::e(dest_size.scale() - from_size.scale());

            vectorize_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
                |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| match x.checked_mul(factor) {
                    Some(x) if x <= max && x >= min => {
                        builder.push(C::compute(&x));
                    }
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            concat!("Decimal overflow at line : ", line!()),
                        );
                        builder.push(T::one());
                    }
                },
            )(buffer, ctx)
        }
        Ordering::Less => {
            let scale_diff = from_size.scale() - dest_size.scale();
            let factor = F::e(scale_diff);
            let scale = from_size.scale();

            vectorize_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
                |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| match decimal_scale_reduction(
                    x,
                    min,
                    max,
                    factor,
                    scale,
                    scale_diff,
                    ctx.func_ctx.rounding_mode,
                ) {
                    Some(y) => builder.push(C::compute(&y)),
                    None => {
                        ctx.set_error(
                            builder.len(),
                            format!("Decimal overflow at line : {}", line!()),
                        );
                        builder.push(T::one());
                    }
                },
            )(buffer, ctx)
        }
    }
}

pub(super) fn decimal_scale_reduction<T: Decimal>(
    x: T,
    min: T,
    max: T,
    factor: T,
    scale: u8,
    scale_diff: u8,
    rounding_mode: bool,
) -> Option<T> {
    let q = x.checked_div(factor)?;
    let y = if let Some(round_val) = get_round_val(x, scale_diff, rounding_mode) {
        q.checked_add(round_val)?
    } else {
        q
    };
    if y > max || y < min || y == T::zero() && !x.int_part_is_zero(scale) {
        None
    } else {
        Some(y)
    }
}

fn decimal_expand_cast<F, T>(
    from_size: DecimalSize,
    dest_size: DecimalSize,
    buffer: Value<DecimalType<F>>,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    F: Decimal,
    T: Decimal + From<F>,
{
    // faster path
    if from_size.scale() == dest_size.scale() && from_size.precision() <= dest_size.precision() {
        return if F::mem_size() == T::mem_size() {
            // 128 -> 128 or 256 -> 256
            buffer.upcast_decimal(dest_size).try_downcast().unwrap()
        } else {
            // 128 -> 256
            vectorize_1_arg::<DecimalType<F>, DecimalType<T>>(|x: F, _: &mut EvalContext| {
                T::from(x)
            })(buffer, ctx)
        };
    }

    if from_size.scale() > dest_size.scale() {
        let scale_diff = from_size.scale() - dest_size.scale();
        let factor = T::e(scale_diff);
        let max = T::max_for_precision(dest_size.precision());
        let min = T::min_for_precision(dest_size.precision());

        vectorize_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
            |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| match decimal_scale_reduction(
                T::from(x),
                min,
                max,
                factor,
                from_size.scale(),
                scale_diff,
                ctx.func_ctx.rounding_mode,
            ) {
                Some(y) => {
                    builder.push(y as T);
                }
                _ => {
                    ctx.set_error(
                        builder.len(),
                        concat!("Decimal overflow at line : ", line!()),
                    );
                    builder.push(T::one());
                }
            },
        )(buffer, ctx)
    } else {
        let factor = T::e(dest_size.scale() - from_size.scale());
        let min = T::min_for_precision(dest_size.precision());
        let max = T::max_for_precision(dest_size.precision());

        vectorize_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
            |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| {
                let x = T::from(x);
                match x.checked_mul(factor) {
                    Some(x) if x <= max && x >= min => {
                        builder.push(x as T);
                    }
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            concat!("Decimal overflow at line : ", line!()),
                        );
                        builder.push(T::one());
                    }
                }
            },
        )(buffer, ctx)
    }
}

pub fn decimal_to_decimal(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    from_type: DecimalDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let from_size = from_type.size();
    let dest_size = dest_type.size();
    match from_type {
        DecimalDataType::Decimal64(_) => {
            let value = arg.try_downcast().unwrap();
            with_decimal_mapped_type!(|OUT| match dest_type {
                DecimalDataType::OUT(_) => {
                    decimal_expand_cast::<i64, OUT>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
            })
        }
        DecimalDataType::Decimal128(_) => {
            let value = arg.try_downcast().unwrap();
            match dest_type {
                DecimalDataType::Decimal64(_) => {
                    decimal_shrink_cast::<i128, i64, I128ToI64>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
                DecimalDataType::Decimal128(_) => {
                    decimal_expand_cast::<i128, i128>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
                DecimalDataType::Decimal256(_) => {
                    decimal_expand_cast::<i128, i256>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
            }
        }
        DecimalDataType::Decimal256(_) => {
            let value = arg.try_downcast().unwrap();
            match dest_type {
                DecimalDataType::Decimal64(_) => {
                    decimal_shrink_cast::<i256, i64, I256ToI64>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
                DecimalDataType::Decimal128(_) => {
                    decimal_shrink_cast::<i256, i128, I256ToI128>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
                DecimalDataType::Decimal256(_) => {
                    decimal_expand_cast::<i256, i256>(from_size, dest_size, value, ctx)
                        .upcast_decimal(dest_size)
                }
            }
        }
    }
}

pub fn decimal_to_decimal_fast(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> (Value<AnyType>, DecimalDataType) {
    let (from_type, _) = DecimalDataType::from_value(arg).unwrap();
    let dest_type = if from_type.scale() != size.scale() {
        size.best_type()
    } else {
        from_type.data_kind().with_size(size)
    };
    let value = decimal_to_decimal(arg, ctx, from_type, dest_type);
    (value, dest_type)
}

trait DecimalConvert<T, U> {
    fn convert(t: T, _scale: i32) -> U;
}

impl DecimalConvert<i128, F32> for F32 {
    fn convert(t: i128, scale: i32) -> F32 {
        let div = 10f32.powi(scale);
        ((t as f32) / div).into()
    }
}

impl DecimalConvert<i128, F64> for F64 {
    fn convert(t: i128, scale: i32) -> F64 {
        let div = 10f64.powi(scale);
        ((t as f64) / div).into()
    }
}

impl DecimalConvert<i256, F32> for F32 {
    fn convert(t: i256, scale: i32) -> F32 {
        let div = 10f32.powi(scale);
        (f32::from(t) / div).into()
    }
}

impl DecimalConvert<i256, F64> for F64 {
    fn convert(t: i256, scale: i32) -> F64 {
        let div = 10f64.powi(scale);
        (f64::from(t) / div).into()
    }
}

fn decimal_to_float<T>(arg: &Value<AnyType>, ctx: &mut EvalContext) -> Value<AnyType>
where
    T: Number,
    T: DecimalConvert<i128, T>,
    T: DecimalConvert<i256, T>,
{
    let (from_type, _) = DecimalDataType::from_value(arg).unwrap();
    match from_type {
        DecimalDataType::Decimal64(size) => {
            let value = arg.try_downcast().unwrap();
            let scale = size.scale() as i32;
            vectorize_1_arg::<Decimal64As128Type, NumberType<T>>(|x, _| T::convert(x, scale))(
                value, ctx,
            )
        }
        DecimalDataType::Decimal128(size) => {
            let value = arg.try_downcast().unwrap();
            let scale = size.scale() as i32;
            vectorize_1_arg::<DecimalType<i128>, NumberType<T>>(|x, _| T::convert(x, scale))(
                value, ctx,
            )
        }
        DecimalDataType::Decimal256(size) => {
            let value = arg.try_downcast().unwrap();
            let scale = size.scale() as i32;
            vectorize_1_arg::<DecimalType<i256>, NumberType<T>>(|x, _| T::convert(x, scale))(
                value, ctx,
            )
        }
    }
    .upcast()
}

fn decimal_to_int<T: Number>(arg: &Value<AnyType>, ctx: &mut EvalContext) -> Value<AnyType> {
    with_decimal_mapped_type!(
        |DECIMAL| match DecimalDataType::from_value(arg).unwrap().0 {
            DecimalDataType::DECIMAL(size) => {
                let value = arg.try_downcast().unwrap();
                vectorize_with_builder_1_arg::<DecimalType<DECIMAL>, NumberType<T>>(
                    |x, builder: &mut Vec<T>, ctx: &mut EvalContext| match x
                        .to_int(size.scale(), ctx.func_ctx.rounding_mode)
                    {
                        Some(x) => builder.push(x),
                        None => {
                            ctx.set_error(builder.len(), "decimal cast to int overflow");
                            builder.push(T::default())
                        }
                    },
                )(value, ctx)
            }
        }
    )
    .upcast()
}

pub fn strict_decimal_data_type(mut data: DataBlock) -> Result<DataBlock, String> {
    use DecimalDataType::*;
    let mut ctx = EvalContext {
        generics: &[],
        num_rows: data.num_rows(),
        func_ctx: &FunctionContext::default(),
        validity: None,
        errors: None,
        suppress_error: false,
        strict_eval: true,
    };
    for entry in data.columns_mut() {
        if entry.value.is_scalar_null() {
            continue;
        }
        let Some((from_type, nullable)) = DecimalDataType::from_value(&entry.value) else {
            continue;
        };

        let size = from_type.size();
        match (size.can_carried_by_128(), from_type.data_kind()) {
            (true, DecimalDataKind::Decimal128) | (false, DecimalDataKind::Decimal256) => continue,
            (true, DecimalDataKind::Decimal64 | DecimalDataKind::Decimal256) => {
                if nullable {
                    let nullable_value =
                        entry.value.try_downcast::<NullableType<AnyType>>().unwrap();
                    let value = nullable_value.value().unwrap();
                    let new_value =
                        decimal_to_decimal(&value, &mut ctx, from_type, Decimal128(size));

                    entry.value =
                        new_value.wrap_nullable(Some(nullable_value.validity(ctx.num_rows)))
                } else {
                    entry.value =
                        decimal_to_decimal(&entry.value, &mut ctx, from_type, Decimal128(size))
                }
            }
            (false, DecimalDataKind::Decimal64 | DecimalDataKind::Decimal128) => {
                if nullable {
                    let nullable_value =
                        entry.value.try_downcast::<NullableType<AnyType>>().unwrap();
                    let value = nullable_value.value().unwrap();
                    let new_value =
                        decimal_to_decimal(&value, &mut ctx, from_type, Decimal256(size));

                    entry.value =
                        new_value.wrap_nullable(Some(nullable_value.validity(ctx.num_rows)))
                } else {
                    entry.value =
                        decimal_to_decimal(&entry.value, &mut ctx, from_type, Decimal256(size))
                }
            }
        }

        if let Some((_, msg)) = ctx.errors.take() {
            return Err(msg);
        }
    }
    Ok(data)
}
