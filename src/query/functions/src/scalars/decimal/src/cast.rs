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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_base::base::OrderedFloat;
use databend_common_expression::BlockEntry;
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
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::i256;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use num_traits::AsPrimitive;

pub(crate) mod typed;
use crate::cast_from_jsonb::variant_to_decimal;
use crate::decimal::DecimalExt;
use crate::vectorize::skip_inactive_with_builder_1_arg;
use crate::vectorize::skip_inactive_with_builder_2_arg;

// int float to decimal
pub fn register_to_decimal(registry: &mut FunctionRegistry) {
    // `try_cast` turns failed rows into NULL. A Variant source already returns
    // a nullable decimal, so its conversion writes NULL directly; every other
    // source maps errors through `Function::error_to_null`.
    let factory = |params: &[Scalar], args_type: &[DataType], try_cast: bool| {
        if args_type.is_empty() || args_type.len() > 4 || params.len() != 2 {
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
        let (args_type, has_format) = match (
            args_type.len(),
            matches!(from_type, DataType::String),
            args_type.len() > 1 && args_type[1].remove_nullable().is_string(),
        ) {
            (1, _, _) => (vec![from_type.clone()], false),
            (2, true, true) => (vec![DataType::String, DataType::String], true),
            (2, true, false) => (
                vec![DataType::String, DataType::Number(NumberDataType::Int64)],
                false,
            ),
            (3, true, true) => (
                vec![
                    DataType::String,
                    DataType::String,
                    DataType::Number(NumberDataType::Int64),
                ],
                true,
            ),
            (3, true, false) => (
                vec![
                    DataType::String,
                    DataType::Number(NumberDataType::Int64),
                    DataType::Number(NumberDataType::Int64),
                ],
                false,
            ),
            (3, false, false) => (
                vec![
                    from_type.clone(),
                    DataType::Number(NumberDataType::Int64),
                    DataType::Number(NumberDataType::Int64),
                ],
                false,
            ),
            (4, true, true) => (
                vec![
                    DataType::String,
                    DataType::String,
                    DataType::Number(NumberDataType::Int64),
                    DataType::Number(NumberDataType::Int64),
                ],
                true,
            ),
            _ => return None,
        };

        let decimal_size =
            DecimalSize::new(params[0].get_i64()? as _, params[1].get_i64()? as _).ok()?;

        let is_variant = from_type == DataType::Variant;
        let return_type = if is_variant {
            DataType::Nullable(Box::new(DataType::Decimal(decimal_size)))
        } else {
            DataType::Decimal(decimal_size)
        };
        let name = if try_cast {
            "try_to_decimal"
        } else {
            "to_decimal"
        };
        let function = Function {
            signature: FunctionSignature {
                name: name.to_string(),
                args_type,
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(move |ctx, d| {
                    if try_cast && is_variant {
                        return FunctionDomain::Full;
                    }
                    if d.len() > 1 {
                        return FunctionDomain::MayThrow;
                    }
                    // Lexicographic string bounds do not bound numeric values
                    // or prove every interior string parses ("10".."2" also
                    // contains "100" and "100x"). Only a singleton is safe.
                    match &d[0] {
                        Domain::String(range) if range.max.as_ref() != Some(&range.min) => {
                            return FunctionDomain::MayThrow;
                        }
                        Domain::Number(NumberDomain::Float32(range))
                            if !range.min.is_finite() || !range.max.is_finite() =>
                        {
                            return FunctionDomain::MayThrow;
                        }
                        Domain::Number(NumberDomain::Float64(range))
                            if !range.min.is_finite() || !range.max.is_finite() =>
                        {
                            return FunctionDomain::MayThrow;
                        }
                        Domain::String(_)
                        | Domain::Number(_)
                        | Domain::Decimal(_)
                        | Domain::Boolean(_) => {}
                        _ => return FunctionDomain::MayThrow,
                    }
                    let decimal_type = DecimalDataType::from(decimal_size);
                    convert_to_decimal_domain(ctx, d[0].clone(), decimal_type)
                        .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                        .unwrap_or(FunctionDomain::MayThrow)
                }),
                eval: scalar_evaluator(move |args, ctx| {
                    let desc_type = DecimalDataType::from(decimal_size);
                    if try_cast && is_variant {
                        return variant_to_decimal_value(&args[0], ctx, desc_type, false, true);
                    }
                    convert_to_decimal(
                        &args[0],
                        has_format.then(|| &args[1]),
                        ctx,
                        &from_type,
                        desc_type,
                    )
                }),
                derive_stat: None,
            },
        };
        Some(if try_cast && !is_variant {
            function.error_to_null()
        } else {
            function
        })
    };

    registry.register_function_factory(
        "to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            Some(Arc::new(factory(params, args_type, false)?))
        })),
    );
    registry.register_function_factory(
        "to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let f = factory(params, args_type, false)?;
            Some(Arc::new(f.passthrough_nullable()))
        })),
    );
    registry.register_aliases("to_decimal", &["to_numeric", "to_number"]);
    registry.register_function_factory(
        "try_to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            Some(Arc::new(factory(params, args_type, true)?))
        })),
    );
    registry.register_function_factory(
        "try_to_decimal",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            let f = factory(params, args_type, true)?;
            Some(Arc::new(f.passthrough_nullable()))
        })),
    );
    registry.register_aliases("try_to_decimal", &["try_to_numeric", "try_to_number"]);

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
            i128::MAX_PRECISION
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
                calc_domain: Box::new(FunctionDomain::Full),
                eval: scalar_evaluator(move |args, ctx| {
                    variant_to_decimal_value(&args[0], ctx, decimal_type, true, false)
                }),
                derive_stat: None,
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
            eval: FunctionEval::Scalar {
                calc_domain,
                eval,
                derive_stat: None,
            },
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
                calc_domain: domain_evaluator(|ctx, d| {
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
                eval: scalar_evaluator(move |args, ctx| decimal_to_int::<T>(&args[0], ctx)),
                derive_stat: None,
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
            [DataType::Nullable(deref!(DataType::Decimal(size)))] => (*size, true),
            [DataType::Decimal(size)] => (*size, false),
            _ => return None,
        };

        let signature = FunctionSignature {
            name: "to_string".to_string(),
            args_type: vec![DataType::Decimal(size)],
            return_type: StringType::data_type(),
        };

        let function = Function::with_passthrough_nullable(
            signature,
            FunctionDomain::Full,
            scalar_evaluator(move |args, ctx| {
                if let Ok(arg) = args[0].try_downcast::<Decimal64Type>() {
                    let arg_type = DecimalDataType::Decimal64(size);
                    return decimal_to_string(arg, arg_type, ctx).upcast();
                };
                if let Ok(arg) = args[0].try_downcast::<Decimal128Type>() {
                    let arg_type = DecimalDataType::Decimal128(size);
                    return decimal_to_string(arg, arg_type, ctx).upcast();
                };
                if let Ok(arg) = args[0].try_downcast::<Decimal256Type>() {
                    let arg_type = DecimalDataType::Decimal256(size);
                    return decimal_to_string(arg, arg_type, ctx).upcast();
                };
                unreachable!()
            }),
            None,
            nullable,
        );
        Some(Arc::new(function))
    };
    registry.register_function_factory("to_string", FunctionFactory::Closure(Box::new(factory)));
}

fn decimal_to_string<T: DecimalExt>(
    arg: Value<DecimalType<T>>,
    from_type: DecimalDataType,
    ctx: &mut EvalContext,
) -> Value<StringType> {
    let scale = from_type.scale();
    vectorize_1_arg::<DecimalType<T>, StringType>(|v, _| v.to_decimal_string(scale))(arg, ctx)
}

pub fn convert_to_decimal(
    input: &Value<AnyType>,
    format: Option<&Value<AnyType>>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    if from_type.is_decimal() {
        let (from_type, _) = DecimalDataType::from_value(input).unwrap();
        decimal_to_decimal(input, ctx, from_type, dest_type)
    } else {
        other_to_decimal(input, format, ctx, from_type, dest_type)
    }
}

pub fn other_to_decimal(
    input: &Value<AnyType>,
    format: Option<&Value<AnyType>>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    if let Some(v) = try {
        let size = dest_type.as_decimal64()?;
        if format.is_some() && size.scale() != 0 || size.precision() < 19 {
            None?
        }
        let buffer = input.as_column()?.as_number()?.as_int64()?;
        Value::<Decimal64Type>::Column(buffer.clone()).upcast_with_type(&DataType::Decimal(*size))
    } {
        return v;
    }

    with_decimal_mapped_type!(|DECIMAL_TYPE| match dest_type {
        DecimalDataType::DECIMAL_TYPE(_) => {
            other_to_decimal_type::<DECIMAL_TYPE>(input, format, ctx, from_type, dest_type)
        }
    })
}

fn other_to_decimal_type<T>(
    input: &Value<AnyType>,
    format: Option<&Value<AnyType>>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType>
where
    T: DecimalExt,
{
    let size = dest_type.size();
    match from_type {
        DataType::Boolean => {
            let input = input.try_downcast().unwrap();
            if size.scale() == size.precision() {
                // With no leading digits only FALSE (zero) is representable.
                skip_inactive_with_builder_1_arg::<BooleanType, DecimalType<T>>(
                    |a, out: &mut Vec<T>, ctx| {
                        if a {
                            ctx.set_error(out.len(), "Decimal overflow");
                            out.push(T::one());
                        } else {
                            out.push(T::zero());
                        }
                    },
                )(input, ctx)
            } else {
                vectorize_1_arg::<BooleanType, DecimalType<T>>(|a: bool, _| {
                    if a { T::e(size.scale()) } else { T::zero() }
                })(input, ctx)
            }
        }

        DataType::Number(ty) => typed::number::<T>(input, *ty, size, ctx),
        DataType::String => {
            let source_arg = input.try_downcast::<StringType>().unwrap();
            let format_arg = format.map(|format| format.try_downcast::<StringType>().unwrap());
            string_to_decimal::<T>(source_arg, format_arg, ctx, size)
        }
        DataType::Variant => {
            let arg = input.try_downcast().unwrap();
            let result = variant_to_decimal::<T>(arg, ctx, dest_type, false, false);
            return result.upcast_with_type(&DataType::Decimal(size).wrap_nullable());
        }
        _ => unreachable!("to_decimal not support this DataType"),
    }
    .upcast_with_type(&DataType::Decimal(size))
}

fn variant_to_decimal_value(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    dest_type: DecimalDataType,
    only_cast_number: bool,
    null_on_error: bool,
) -> Value<AnyType> {
    let size = dest_type.size();
    with_decimal_mapped_type!(|DECIMAL_TYPE| match dest_type {
        DecimalDataType::DECIMAL_TYPE(_) => {
            let arg = arg.try_downcast().unwrap();
            variant_to_decimal::<DECIMAL_TYPE>(arg, ctx, dest_type, only_cast_number, null_on_error)
                .upcast_with_type(&DataType::Decimal(size).wrap_nullable())
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
                    NumberType::<NUM_TYPE>::from_data(vec![d.min, d.max])
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
                    DecimalType::from_data_with_size(vec![d.min, d.max], Some(size))
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
        strict_eval: false,
    };
    let dest_size = dest_type.size();
    let res = convert_to_decimal(&value, None, &mut ctx, &from_type, dest_type);

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

fn string_to_decimal<T: DecimalExt>(
    from: Value<StringType>,
    format: Option<Value<StringType>>,
    ctx: &mut EvalContext,
    decimal_size: DecimalSize,
) -> Value<DecimalType<T>> {
    let parse =
        |source: &str, format: Option<&str>, builder: &mut Vec<T>, ctx: &mut EvalContext| {
            let source = source.trim();
            let value = match format {
                None => Cow::Borrowed(source),
                Some(format) => match decimal_format(source, format) {
                    Ok(value) => Cow::Owned(value),
                    Err(error) => {
                        ctx.set_error(builder.len(), error);
                        builder.push(T::zero());
                        return;
                    }
                },
            };
            match read_decimal_with_size::<T>(
                value.as_bytes(),
                decimal_size,
                true,
                ctx.func_ctx.rounding_mode,
            ) {
                Ok((value, _)) => builder.push(value),
                Err(error) => {
                    ctx.set_error(builder.len(), error);
                    builder.push(T::zero());
                }
            }
        };
    match format {
        None => {
            skip_inactive_with_builder_1_arg::<StringType, DecimalType<T>>(|source, out, ctx| {
                parse(source, None, out, ctx)
            })(from, ctx)
        }
        Some(format) => skip_inactive_with_builder_2_arg::<StringType, StringType, DecimalType<T>>(
            |source, format, out, ctx| parse(source, Some(format), out, ctx),
        )(from, format, ctx),
    }
}

fn integer_to_decimal<T, S>(
    from: Value<S>,
    ctx: &mut EvalContext,
    size: DecimalSize,
) -> Value<DecimalType<T>>
where
    T: DecimalExt,
    S: AccessType,
    for<'a> S::ScalarRef<'a>: Number + Into<i128>,
{
    let multiplier = T::e(size.scale());

    let min_for_precision = T::min_for_precision(size.precision());
    let max_for_precision = T::max_for_precision(size.precision());
    let range = min_for_precision..=max_for_precision;

    // Scale zero avoids rescaling, not precision overflow. Only omit row
    // checks when the entire source integer range fits the decimal target.
    let never_overflow = [
        <S::ScalarRef<'_> as Number>::MIN,
        <S::ScalarRef<'_> as Number>::MAX,
    ]
    .into_iter()
    .all(|x| {
        T::from_i128(x)
            .and_then(|x| x.checked_mul(multiplier))
            .is_some_and(|x| range.contains(&x))
    });

    if never_overflow {
        vectorize_1_arg(|x: S::ScalarRef<'_>, _| T::from_i128_uncheck(x.into()) * multiplier)(
            from, ctx,
        )
    } else {
        let overflow = concat!("Decimal overflow at line : ", line!());
        if let Value::Scalar(value) = &from {
            let x = S::to_scalar_ref(value);
            if let Some(x) = T::from_i128(x).and_then(|x| x.checked_mul(multiplier))
                && range.contains(&x)
            {
                return Value::Scalar(x);
            }
            // Scalar casts broadcast too: row zero may be inactive while later
            // rows still need the error. EvalContext filters invisible rows.
            for row in 0..ctx.num_rows {
                ctx.set_error(row, overflow);
            }
            return Value::Scalar(T::one());
        }
        let f = |x: S::ScalarRef<'_>, builder: &mut Vec<T>, ctx: &mut EvalContext| {
            if let Some(x) = T::from_i128(x).and_then(|x| x.checked_mul(multiplier))
                && range.contains(&x)
            {
                builder.push(x);
            } else {
                ctx.set_error(builder.len(), overflow);
                builder.push(T::one());
            }
        };
        skip_inactive_with_builder_1_arg(f)(from, ctx)
    }
}

fn float_to_decimal<T: DecimalExt, S: AccessType>(
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

    skip_inactive_with_builder_1_arg(f)(from, ctx)
}

/// Decimal to decimal cast from storage `F` to `T`, computing in `W`, the
/// wider of the two. The scale relation alone selects the row operation.
pub(super) fn decimal_cast<F, W, T>(
    from: DecimalSize,
    dest: DecimalSize,
    value: Value<DecimalType<F>>,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    F: DecimalExt,
    W: DecimalExt,
    T: DecimalExt,
{
    const { assert!(F::MAX_PRECISION <= W::MAX_PRECISION && T::MAX_PRECISION <= W::MAX_PRECISION) };
    if from.scale() == dest.scale() && from.precision() <= dest.precision() {
        // Every value fits; storage can only stay or widen.
        return if F::KIND == T::KIND {
            value
                .upcast_with_type(&DataType::Decimal(dest))
                .try_downcast()
                .unwrap()
        } else {
            vectorize_1_arg::<DecimalType<F>, DecimalType<T>>(|x, _| x.as_decimal::<T>())(
                value, ctx,
            )
        };
    }

    let min = W::min_for_precision(dest.precision());
    let max = W::max_for_precision(dest.precision());
    match dest.scale().cmp(&from.scale()) {
        Ordering::Equal => {
            rescale::<F, W, T>(value, ctx, move |x, _| (x >= min && x <= max).then_some(x))
        }
        Ordering::Greater => {
            let factor = W::e(dest.scale() - from.scale());
            rescale::<F, W, T>(value, ctx, move |x, _| {
                x.checked_mul(factor).filter(|x| *x >= min && *x <= max)
            })
        }
        Ordering::Less => {
            let reduction = ScaleReduction::<W>::new(from.scale() - dest.scale(), dest.precision());
            rescale::<F, W, T>(value, ctx, move |x, rounding_mode| {
                reduction.reduce(x, rounding_mode)
            })
        }
    }
}

// Rows the rescaling rejects do not fit the target precision.
fn rescale<F, W, T>(
    value: Value<DecimalType<F>>,
    ctx: &mut EvalContext,
    rescale: impl Fn(W, bool) -> Option<W> + Copy + Send + Sync,
) -> Value<DecimalType<T>>
where
    F: DecimalExt,
    W: DecimalExt,
    T: DecimalExt,
{
    skip_inactive_with_builder_1_arg::<DecimalType<F>, DecimalType<T>>(
        move |x: F, builder: &mut Vec<T>, ctx: &mut EvalContext| match rescale(
            x.as_decimal::<W>(),
            ctx.func_ctx.rounding_mode,
        ) {
            Some(y) => builder.push(y.as_decimal::<T>()),
            None => {
                ctx.set_error(
                    builder.len(),
                    concat!("Decimal overflow at line : ", line!()),
                );
                builder.push(T::one());
            }
        },
    )(value, ctx)
}

/// Drops `shift >= 1` fractional digits, truncating or rounding half away
/// from zero, then checks the target range.
///
/// The only failure is leaving the range: the divisor is a positive power of
/// ten, and a rounded quotient has at most `|x| / 10 + 1` magnitude, so
/// neither can overflow. A value with a nonzero integer part never reduces to
/// zero either, as at least the target scale's unit remains.
#[derive(Clone, Copy, Debug)]
pub(super) struct ScaleReduction<T> {
    factor: T,
    half: T,
    min: T,
    max: T,
}

impl<T: DecimalExt> ScaleReduction<T> {
    pub(super) fn new(shift: u8, precision: u8) -> Self {
        assert!(shift >= 1, "internal scale reduction by {shift} digits");
        let factor = T::e(shift);
        Self {
            factor,
            half: factor / T::from_i64(2),
            min: T::min_for_precision(precision),
            max: T::max_for_precision(precision),
        }
    }

    #[inline]
    pub(super) fn reduce(&self, x: T, rounding_mode: bool) -> Option<T> {
        let q = x / self.factor;
        let y = if !rounding_mode {
            q
        } else {
            let r = x % self.factor;
            if r >= self.half {
                q + T::one()
            } else if r <= -self.half {
                q - T::one()
            } else {
                q
            }
        };
        (self.min <= y && y <= self.max).then_some(y)
    }
}

pub fn decimal_to_decimal(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    from_type: DecimalDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    match dest_type {
        DecimalDataType::Decimal64(size) => {
            typed::decimal64(from_type, size, arg, ctx).upcast_with_type(&DataType::Decimal(size))
        }
        DecimalDataType::Decimal128(size) => {
            typed::decimal128(from_type, size, arg, ctx).upcast_with_type(&DataType::Decimal(size))
        }
        DecimalDataType::Decimal256(size) => {
            typed::decimal256(from_type, size, arg, ctx).upcast_with_type(&DataType::Decimal(size))
        }
    }
}

// Delegates to `Decimal::to_float32`/`to_float64` so that values and domains
// (see `register_decimal_to_float`) use the same correctly rounded conversion.
trait DecimalConvert<T, U> {
    fn convert(t: T, scale: u8) -> U;
}

impl DecimalConvert<i128, F32> for F32 {
    fn convert(t: i128, scale: u8) -> F32 {
        t.to_float32(scale).into()
    }
}

impl DecimalConvert<i128, F64> for F64 {
    fn convert(t: i128, scale: u8) -> F64 {
        t.to_float64(scale).into()
    }
}

impl DecimalConvert<i256, F32> for F32 {
    fn convert(t: i256, scale: u8) -> F32 {
        t.to_float32(scale).into()
    }
}

impl DecimalConvert<i256, F64> for F64 {
    fn convert(t: i256, scale: u8) -> F64 {
        t.to_float64(scale).into()
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
            let scale = size.scale();
            vectorize_1_arg::<Decimal64As128Type, NumberType<T>>(|x, _| T::convert(x, scale))(
                value, ctx,
            )
        }
        DecimalDataType::Decimal128(size) => {
            let value = arg.try_downcast().unwrap();
            let scale = size.scale();
            vectorize_1_arg::<DecimalType<i128>, NumberType<T>>(|x, _| T::convert(x, scale))(
                value, ctx,
            )
        }
        DecimalDataType::Decimal256(size) => {
            let value = arg.try_downcast().unwrap();
            let scale = size.scale();
            vectorize_1_arg::<DecimalType<i256>, NumberType<T>>(|x, _| T::convert(x, scale))(
                value, ctx,
            )
        }
    }
    .upcast_with_type(&DataType::Number(T::data_type()))
}

fn decimal_to_int<T: Number>(arg: &Value<AnyType>, ctx: &mut EvalContext) -> Value<AnyType> {
    with_decimal_mapped_type!(
        |DECIMAL| match DecimalDataType::from_value(arg).unwrap().0 {
            DecimalDataType::DECIMAL(size) => {
                let value = arg.try_downcast().unwrap();
                skip_inactive_with_builder_1_arg::<DecimalType<DECIMAL>, NumberType<T>>(
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
    .upcast_with_type(&DataType::Number(T::data_type()))
}

pub fn strict_decimal_data_type(data: DataBlock) -> Result<DataBlock, String> {
    let num_rows = data.num_rows();
    let entries = data
        .take_columns()
        .into_iter()
        .map(|entry| {
            let value = entry.value();
            if value.is_scalar_null() {
                return Ok(entry);
            }

            let value = strict_decimal_data_type_value(value)?;
            Ok(BlockEntry::new(value, || (entry.data_type(), entry.len())))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(DataBlock::new(entries, num_rows))
}

fn strict_decimal_data_type_value(value: Value<AnyType>) -> Result<Value<AnyType>, String> {
    use DecimalDataType::*;
    // todo array map
    // if let Some(array) = value.try_downcast::<ArrayType<AnyType>>() { }

    let Some((from_type, nullable)) = DecimalDataType::from_value(&value) else {
        return Ok(value);
    };

    let mut ctx = EvalContext {
        generics: &[],
        num_rows: value.len(),
        func_ctx: &FunctionContext::default(),
        validity: None,
        errors: None,
        suppress_error: false,
        strict_eval: true,
    };

    let size = from_type.size();
    let to_kind = size.data_kind();

    if from_type.data_kind() == to_kind {
        return Ok(value);
    }

    let value = with_decimal_type!(|DECIMAL| match to_kind {
        DecimalDataKind::DECIMAL => {
            if nullable {
                let nullable_value = value.try_downcast::<NullableType<AnyType>>().unwrap();
                let value = nullable_value.value().unwrap();
                let new_value = decimal_to_decimal(&value, &mut ctx, from_type, DECIMAL(size));

                new_value.wrap_nullable(Some(nullable_value.validity(ctx.num_rows)))
            } else {
                decimal_to_decimal(&value, &mut ctx, from_type, DECIMAL(size))
            }
        }
    });

    if let Some((_, msg)) = ctx.errors.take() {
        Err(msg)
    } else {
        Ok(value)
    }
}

fn decimal_format(input: &str, format: &str) -> Result<String, &'static str> {
    let mut result = String::new();

    let mut input_chars = input.chars().peekable();
    let mut is_negative = false;

    for fmt_char in format.chars() {
        match fmt_char {
            '0' | '9' => match input_chars.next() {
                Some(c) if c.is_ascii_digit() => result.push(c),
                Some(_) => return Err("Expected digit"),
                None => return Err("Unexpected end of input"),
            },
            'G' => match input_chars.next() {
                Some(',') => {}
                Some(_) => return Err("Expected group separator ','"),
                None => return Err("Unexpected end of input"),
            },
            'D' => match input_chars.next() {
                Some('.') | Some(',') => result.push('.'),
                Some(_) => return Err("Expected decimal point"),
                None => return Err("Unexpected end of input"),
            },
            'S' => match input_chars.peek() {
                Some('+') => {
                    input_chars.next();
                }
                Some('-') => {
                    input_chars.next();
                    is_negative = true;
                }
                Some(_) => return Err("Expected '+' or '-' for sign"),
                None => return Err("Unexpected end of input"),
            },
            expected => match input_chars.next() {
                Some(c) if c == expected => {}
                Some(_) => return Err("Unexpected character"),
                None => return Err("Unexpected end of input"),
            },
        }
    }

    if input_chars.next().is_some() {
        return Err("Input is longer than expected");
    }
    let trimmed_result = result.trim_start_matches('0');
    let final_result = if trimmed_result.is_empty() {
        "0".to_string()
    } else {
        trimmed_result.to_string()
    };

    if is_negative {
        Ok(format!("-{}", final_result))
    } else {
        Ok(final_result)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberScalar;

    use super::*;
    use crate::testing::*;

    fn sizes() -> Vec<DecimalSize> {
        let mut sizes = vec![];
        for p in [1, 2, 18, 19, 38, 39, 76] {
            sizes.extend(
                scales(p, [0, 1, p / 2, p - 1, p]).map(|s| DecimalSize::new(p, s).unwrap()),
            );
        }
        sizes
    }

    // Every pairing of source and target storage and every scale relation,
    // over source edge values, once without validity and once with every third
    // row hidden behind storage extremes. Scale reduction truncates, the
    // default rounding mode.
    #[test]
    fn decimal_cast_matches_bigint_on_edges() {
        let mut registry = FunctionRegistry::empty();
        register_to_decimal(&mut registry);
        for from in sizes() {
            let ty = DataType::Decimal(from);
            let values = edges(&ty);
            let payloads = hidden_payloads(&ty);
            let visible = (0..values.len())
                .map(|row| row % 3 != 0)
                .collect::<Vec<_>>();
            for dest in sizes() {
                let params = [dest.precision() as i64, dest.scale() as i64]
                    .map(|v| Scalar::Number(NumberScalar::Int64(v)))
                    .to_vec();
                let function =
                    bind_function(&registry, "to_decimal", params, std::slice::from_ref(&ty));
                for visible in [None, Some(&visible[..])] {
                    let rows = (0..values.len())
                        .map(|row| match visible {
                            Some(visible) if !visible[row] => payloads[row % 2].clone(),
                            _ => values[row].clone(),
                        })
                        .collect::<Vec<_>>();
                    let expected = rows
                        .iter()
                        .map(|value| {
                            let value = if dest.scale() >= from.scale() {
                                value * pow10((dest.scale() - from.scale()) as u32)
                            } else {
                                value / pow10((from.scale() - dest.scale()) as u32)
                            };
                            fits(&value, dest.precision()).then_some(value)
                        })
                        .collect::<Vec<Expected>>();
                    let args = [Value::Column(column(&ty, &rows))];
                    check_rows(&function, &args, visible, &expected, |row| {
                        format!("{from:?} -> {dest:?}, row {row}: {}", rows[row])
                    });
                }
            }
        }
    }
}
