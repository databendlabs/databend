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

use std::ops::Mul;
use std::sync::Arc;

use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use ethnum::i256;
use num_traits::AsPrimitive;
use ordered_float::OrderedFloat;

// int float to decimal
pub fn register_to_decimal(registry: &mut FunctionRegistry) {
    let factory = |params: &[Scalar], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }
        if params.len() != 2 {
            return None;
        }

        let from_type = args_type[0].remove_nullable();

        if !matches!(
            from_type,
            DataType::Boolean | DataType::Number(_) | DataType::Decimal(_) | DataType::String
        ) {
            return None;
        }

        let decimal_size = DecimalSize {
            precision: params[0].get_i64()? as _,
            scale: params[1].get_i64()? as _,
        };

        let decimal_type = DecimalDataType::from_size(decimal_size).ok()?;

        Some(Function {
            signature: FunctionSignature {
                name: "to_decimal".to_string(),
                args_type: vec![from_type.clone()],
                return_type: DataType::Decimal(decimal_type),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |ctx, d| {
                    convert_to_decimal_domain(ctx, d[0].clone(), decimal_type)
                        .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                        .unwrap_or(FunctionDomain::MayThrow)
                }),
                eval: Box::new(move |args, ctx| {
                    convert_to_decimal(&args[0], ctx, &from_type, decimal_type)
                }),
            },
        })
    };

    registry.register_function_factory("to_decimal", move |params, args_type| {
        Some(Arc::new(factory(params, args_type)?))
    });
    registry.register_function_factory("to_decimal", move |params, args_type| {
        let f = factory(params, args_type)?;
        Some(Arc::new(f.passthrough_nullable()))
    });
    registry.register_function_factory("try_to_decimal", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_decimal".to_string();
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory("try_to_decimal", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_decimal".to_string();
        Some(Arc::new(f.error_to_null().passthrough_nullable()))
    });
}

pub(crate) fn register_decimal_to_float<T: Number>(registry: &mut FunctionRegistry) {
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
                                min: OrderedFloat(d.min.to_float32(size.scale)),
                                max: OrderedFloat(d.max.to_float32(size.scale)),
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
                                min: OrderedFloat(d.min.to_float64(size.scale)),
                                max: OrderedFloat(d.max.to_float64(size.scale)),
                            },
                        )))
                    }
                })
            }) as _
        };

        let eval = if is_f32 {
            let arg_type = arg_type.clone();
            Box::new(move |args: &[ValueRef<AnyType>], tx: &mut EvalContext| {
                decimal_to_float::<F32>(&args[0], arg_type.clone(), tx)
            }) as _
        } else {
            let arg_type = arg_type.clone();
            Box::new(move |args: &[ValueRef<AnyType>], tx: &mut EvalContext| {
                decimal_to_float::<F64>(&args[0], arg_type.clone(), tx)
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

    registry.register_function_factory(name, move |params, args_type| {
        let data_type = NumberType::<T>::data_type();
        Some(Arc::new(factory(params, args_type, data_type)?))
    });
    registry.register_function_factory(name, move |params, args_type| {
        let data_type = NumberType::<T>::data_type();
        let f = factory(params, args_type, data_type)?;
        Some(Arc::new(f.passthrough_nullable()))
    });
    registry.register_function_factory(&format!("try_{name}"), move |params, args_type| {
        let data_type = NumberType::<T>::data_type();
        let mut f = factory(params, args_type, data_type)?;
        f.signature.name = format!("try_{name}");
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory(&format!("try_{name}"), move |params, args_type| {
        let data_type = NumberType::<T>::data_type();
        let mut f = factory(params, args_type, data_type)?;
        f.signature.name = format!("try_{name}");
        Some(Arc::new(f.error_to_null().passthrough_nullable()))
    });
}

pub(crate) fn register_decimal_to_int<T: Number>(registry: &mut FunctionRegistry) {
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
                        DecimalDomain::Decimal128(d, size) => Some(SimpleDomain::<T> {
                            min: d.min.to_int(size.scale, ctx.rounding_mode)?,
                            max: d.max.to_int(size.scale, ctx.rounding_mode)?,
                        }),
                        DecimalDomain::Decimal256(d, size) => Some(SimpleDomain::<T> {
                            min: d.min.to_int(size.scale, ctx.rounding_mode)?,
                            max: d.max.to_int(size.scale, ctx.rounding_mode)?,
                        }),
                    };

                    res_fn()
                        .map(|d| FunctionDomain::Domain(Domain::Number(T::upcast_domain(d))))
                        .unwrap_or(FunctionDomain::MayThrow)
                }),
                eval: Box::new(move |args, tx| decimal_to_int::<T>(&args[0], arg_type.clone(), tx)),
            },
        };

        Some(function)
    };

    registry.register_function_factory(&name, move |params, args_type| {
        Some(Arc::new(factory(params, args_type)?))
    });
    registry.register_function_factory(&name, move |params, args_type| {
        let f = factory(params, args_type)?;
        Some(Arc::new(f.passthrough_nullable()))
    });
    registry.register_function_factory(&try_name, move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = format!("try_to_{}", T::data_type().to_string().to_lowercase());
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory(&try_name, move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = format!("try_to_{}", T::data_type().to_string().to_lowercase());
        Some(Arc::new(f.error_to_null().passthrough_nullable()))
    });
}

pub(crate) fn register_decimal_to_string(registry: &mut FunctionRegistry) {
    // decimal to string
    let factory = |_params: &[Scalar], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }

        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }

        let function = Function {
            signature: FunctionSignature {
                name: "to_string".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: StringType::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, tx| decimal_to_string(args, arg_type.clone(), tx)),
            },
        };

        if args_type[0].is_nullable() {
            Some(Arc::new(function.passthrough_nullable()))
        } else {
            Some(Arc::new(function))
        }
    };
    registry.register_function_factory("to_string", factory);
}

fn decimal_to_string(
    args: &[ValueRef<AnyType>],
    from_type: DataType,
    _ctx: &mut EvalContext,
) -> Value<AnyType> {
    let arg = &args[0];
    let from_type = from_type.as_decimal().unwrap();

    with_decimal_mapped_type!(|DECIMAL_TYPE| match from_type {
        DecimalDataType::DECIMAL_TYPE(from_size) => {
            let arg: ValueRef<DecimalType<DECIMAL_TYPE>> = arg.try_downcast().unwrap();

            match arg {
                ValueRef::Column(col) => {
                    let mut builder = StringColumnBuilder::with_capacity(col.len(), col.len() * 10);
                    for x in DecimalType::<DECIMAL_TYPE>::iter_column(&col) {
                        builder.put_str(&DECIMAL_TYPE::display(x, from_size.scale));
                        builder.commit_row();
                    }
                    Value::Column(Column::String(builder.build()))
                }
                ValueRef::Scalar(x) => Value::Scalar(Scalar::String(
                    DECIMAL_TYPE::display(x, from_size.scale).into(),
                )),
            }
        }
    })
}

pub fn convert_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    if let DataType::Decimal(f) = from_type {
        return decimal_to_decimal(arg, ctx, *f, dest_type);
    }

    with_decimal_mapped_type!(|DECIMAL_TYPE| match dest_type {
        DecimalDataType::DECIMAL_TYPE(size) => {
            type T = DECIMAL_TYPE;
            let result = match from_type {
                DataType::Boolean => {
                    let arg = arg.try_downcast().unwrap();
                    vectorize_1_arg::<BooleanType, DecimalType<T>>(|a: bool, _| {
                        if a {
                            T::e(size.scale as u32)
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
                _ => unreachable!("to_decimal not support this DataType"),
            };
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
                    DecimalType::from_data_with_size(vec![min, max], size)
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
    };
    let dest_size = dest_type.size();
    let res = convert_to_decimal(&value.as_ref(), &mut ctx, &from_type, dest_type);

    if ctx.errors.is_some() {
        return None;
    }
    let decimal_col = res.as_column()?.as_decimal()?;
    assert_eq!(decimal_col.len(), 2);

    Some(match decimal_col {
        DecimalColumn::Decimal128(buf, size) => {
            assert_eq!(&dest_size, size);
            let (min, max) = unsafe { (*buf.get_unchecked(0), *buf.get_unchecked(1)) };
            DecimalDomain::Decimal128(SimpleDomain { min, max }, *size)
        }
        DecimalColumn::Decimal256(buf, size) => {
            assert_eq!(&dest_size, size);
            let (min, max) = unsafe { (*buf.get_unchecked(0), *buf.get_unchecked(1)) };
            DecimalDomain::Decimal256(SimpleDomain { min, max }, *size)
        }
    })
}

fn string_to_decimal<T>(
    from: ValueRef<StringType>,
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
                    ctx.set_error(builder.len(), e.message());
                    T::zero()
                }
            };

        builder.push(value);
    };

    vectorize_with_builder_1_arg::<StringType, DecimalType<T>>(f)(from, ctx)
}

fn integer_to_decimal<T, S>(
    from: ValueRef<S>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> Value<DecimalType<T>>
where
    T: Decimal + Mul<Output = T>,
    S: ArgType,
    for<'a> S::ScalarRef<'a>: Number + AsPrimitive<i128>,
{
    let multiplier = T::e(size.scale as u32);

    let min_for_precision = T::min_for_precision(size.precision);
    let max_for_precision = T::max_for_precision(size.precision);

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

fn float_to_decimal<T: Decimal, S: ArgType>(
    from: ValueRef<S>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> Value<DecimalType<T>>
where
    for<'a> S::ScalarRef<'a>: Number + AsPrimitive<f64>,
{
    let multiplier: f64 = (10_f64).powi(size.scale as i32).as_();

    let min_for_precision = T::min_for_precision(size.precision);
    let max_for_precision = T::max_for_precision(size.precision);

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
fn get_round_val<T: Decimal>(x: T, scale: u32, ctx: &mut EvalContext) -> Option<T> {
    let mut round_val = None;
    if ctx.func_ctx.rounding_mode && scale > 0 {
        // Checking whether numbers need to be added or subtracted to calculate rounding
        if let Some(r) = x.checked_rem(T::e(scale)) {
            if let Some(m) = r.checked_div(T::e(scale - 1)) {
                if m >= T::from_i128(5i64) {
                    round_val = Some(T::one());
                } else if m <= T::from_i128(-5i64) {
                    round_val = Some(T::minus_one());
                }
            }
        }
    }
    round_val
}

fn decimal_256_to_128(
    buffer: &ValueRef<AnyType>,
    from_size: DecimalSize,
    dest_size: DecimalSize,
    ctx: &mut EvalContext,
) -> Value<DecimalType<i128>> {
    let max = i128::max_for_precision(dest_size.precision);
    let min = i128::min_for_precision(dest_size.precision);

    let buffer = buffer.try_downcast::<DecimalType<i256>>().unwrap();
    if dest_size.scale >= from_size.scale {
        let factor = i256::e((dest_size.scale - from_size.scale) as u32);

        vectorize_with_builder_1_arg::<DecimalType<i256>, DecimalType<i128>>(
            |x: i256, builder: &mut Vec<i128>, ctx: &mut EvalContext| match x.checked_mul(factor) {
                Some(x) if x <= max && x >= min => builder.push(*x.low()),
                _ => {
                    ctx.set_error(
                        builder.len(),
                        concat!("Decimal overflow at line : ", line!()),
                    );
                    builder.push(i128::one());
                }
            },
        )(buffer, ctx)
    } else {
        let scale_diff = (from_size.scale - dest_size.scale) as u32;
        let factor = i256::e(scale_diff);
        let source_factor = i256::e(from_size.scale as u32);

        vectorize_with_builder_1_arg::<DecimalType<i256>, DecimalType<i128>>(
            |x: i256, builder: &mut Vec<i128>, ctx: &mut EvalContext| {
                let round_val = get_round_val::<i256>(x, scale_diff, ctx);
                let y = match (x.checked_div(factor), round_val) {
                    (Some(x), Some(round_val)) => x.checked_add(round_val),
                    (Some(x), None) => Some(x),
                    (None, _) => None,
                };

                match y {
                    Some(y) if (y <= max && y >= min) && (y != 0 || x / source_factor == 0) => {
                        builder.push(*y.low());
                    }
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            concat!("Decimal overflow at line : ", line!()),
                        );

                        builder.push(i128::one());
                    }
                }
            },
        )(buffer, ctx)
    }
}

macro_rules! m_decimal_to_decimal {
    ($from_size: expr, $dest_size: expr, $value: expr, $from_type_name: ty, $dest_type_name: ty, $ctx: expr) => {
        type F = $from_type_name;
        type T = $dest_type_name;

        let buffer: ValueRef<DecimalType<F>> = $value.try_downcast().unwrap();
        // faster path
        let result: Value<DecimalType<T>> = if $from_size.scale == $dest_size.scale
            && $from_size.precision <= $dest_size.precision
        {
            if F::MAX == T::MAX {
                // 128 -> 128 or 256 -> 256
                return buffer.clone().to_owned().upcast_decimal($dest_size);
            } else {
                // 128 -> 256
                vectorize_1_arg::<DecimalType<F>, DecimalType<T>>(|x: F, _: &mut EvalContext| {
                    T::from(x)
                })(buffer, $ctx)
            }
        } else if $from_size.scale > $dest_size.scale {
            let scale_diff = ($from_size.scale - $dest_size.scale) as u32;
            let factor = T::e(scale_diff);
            let max = T::max_for_precision($dest_size.precision);
            let min = T::min_for_precision($dest_size.precision);

            let source_factor = F::e($from_size.scale as u32);

            vectorize_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
                |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| {
                    let x = T::from(x);
                    let round_val = get_round_val::<T>(x, scale_diff, ctx);
                    let y = match (x.checked_div(factor), round_val) {
                        (Some(x), Some(round_val)) => x.checked_add(round_val),
                        (Some(x), None) => Some(x),
                        (None, _) => None,
                    };

                    match y {
                        Some(y) if y <= max && y >= min && (y != 0 || x / source_factor == 0) => {
                            builder.push(y as T);
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
            )(buffer, $ctx)
        } else {
            let factor = T::e(($dest_size.scale - $from_size.scale) as u32);
            let min = T::min_for_precision($dest_size.precision);
            let max = T::max_for_precision($dest_size.precision);

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
            )(buffer, $ctx)
        };

        result.upcast_decimal($dest_size)
    };
}

fn decimal_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: DecimalDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let from_size = from_type.size();
    let dest_size = dest_type.size();
    match (from_type, dest_type) {
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal128(_)) => {
            m_decimal_to_decimal! {from_size, dest_size, arg, i128, i128, ctx}
        }
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal256(_)) => {
            m_decimal_to_decimal! {from_size, dest_size, arg, i128, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal128(_)) => {
            let value = decimal_256_to_128(arg, from_size, dest_size, ctx);
            value.upcast_decimal(dest_size)
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal256(_)) => {
            m_decimal_to_decimal! {from_size, dest_size, arg, i256, i256, ctx}
        }
    }
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

fn decimal_to_float<T>(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    ctx: &mut EvalContext,
) -> Value<AnyType>
where
    T: Number,
    T: DecimalConvert<i128, T>,
    T: DecimalConvert<i256, T>,
{
    let from_type = from_type.as_decimal().unwrap();

    let result = with_decimal_mapped_type!(|DECIMAL_TYPE| match from_type {
        DecimalDataType::DECIMAL_TYPE(from_size) => {
            let value = arg.try_downcast().unwrap();
            let scale = from_size.scale as i32;
            vectorize_1_arg::<DecimalType<DECIMAL_TYPE>, NumberType<T>>(
                |x, _ctx: &mut EvalContext| T::convert(x, scale),
            )(value, ctx)
        }
    });

    result.upcast()
}

fn decimal_to_int<T: Number>(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    ctx: &mut EvalContext,
) -> Value<AnyType> {
    let from_type = from_type.as_decimal().unwrap();

    let result = with_decimal_mapped_type!(|DECIMAL_TYPE| match from_type {
        DecimalDataType::DECIMAL_TYPE(from_size) => {
            let value = arg.try_downcast().unwrap();
            vectorize_with_builder_1_arg::<DecimalType<DECIMAL_TYPE>, NumberType<T>>(
                |x, builder: &mut Vec<T>, ctx: &mut EvalContext| match x
                    .to_int(from_size.scale, ctx.func_ctx.rounding_mode)
                {
                    Some(x) => builder.push(x),
                    None => {
                        ctx.set_error(builder.len(), "decimal cast to int overflow");
                        builder.push(T::default())
                    }
                },
            )(value, ctx)
        }
    });

    result.upcast()
}
