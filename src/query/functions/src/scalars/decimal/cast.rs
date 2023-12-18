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

use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
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
    let factory = |params: &[usize], args_type: &[DataType]| {
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
            precision: params[0] as u8,
            scale: params[1] as u8,
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

pub(crate) fn register_decimal_to_float64(registry: &mut FunctionRegistry) {
    let factory = |_params: &[usize], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }

        let arg_type = args_type[0].remove_nullable();

        if !arg_type.is_decimal() {
            return None;
        }

        let function = Function {
            signature: FunctionSignature {
                name: "to_float64".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: Float64Type::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, d| match d[0].as_decimal().unwrap() {
                    DecimalDomain::Decimal128(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float64(SimpleDomain {
                            min: OrderedFloat(d.min.to_float64(size.scale)),
                            max: OrderedFloat(d.max.to_float64(size.scale)),
                        }),
                    )),
                    DecimalDomain::Decimal256(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float64(SimpleDomain {
                            min: OrderedFloat(d.min.to_float64(size.scale)),
                            max: OrderedFloat(d.max.to_float64(size.scale)),
                        }),
                    )),
                }),
                eval: Box::new(move |args, tx| decimal_to_float64(&args[0], arg_type.clone(), tx)),
            },
        };

        Some(function)
    };

    registry.register_function_factory("to_float64", move |params, args_type| {
        Some(Arc::new(factory(params, args_type)?))
    });
    registry.register_function_factory("to_float64", move |params, args_type| {
        let f = factory(params, args_type)?;
        Some(Arc::new(f.passthrough_nullable()))
    });
    registry.register_function_factory("try_to_float64", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_float64".to_string();
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory("try_to_float64", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_float64".to_string();
        Some(Arc::new(f.error_to_null().passthrough_nullable()))
    });
}

pub(crate) fn register_decimal_to_float32(registry: &mut FunctionRegistry) {
    let factory = |_params: &[usize], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }

        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }

        let function = Function {
            signature: FunctionSignature {
                name: "to_float32".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: Float32Type::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, d| match d[0].as_decimal().unwrap() {
                    DecimalDomain::Decimal128(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float32(SimpleDomain {
                            min: OrderedFloat(d.min.to_float32(size.scale)),
                            max: OrderedFloat(d.max.to_float32(size.scale)),
                        }),
                    )),
                    DecimalDomain::Decimal256(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float32(SimpleDomain {
                            min: OrderedFloat(d.min.to_float32(size.scale)),
                            max: OrderedFloat(d.max.to_float32(size.scale)),
                        }),
                    )),
                }),
                eval: Box::new(move |args, tx| decimal_to_float32(&args[0], arg_type.clone(), tx)),
            },
        };

        Some(function)
    };

    registry.register_function_factory("to_float32", move |params, args_type| {
        Some(Arc::new(factory(params, args_type)?))
    });
    registry.register_function_factory("to_float32", move |params, args_type| {
        let f = factory(params, args_type)?;
        Some(Arc::new(f.passthrough_nullable()))
    });
    registry.register_function_factory("try_to_float32", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_float32".to_string();
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory("try_to_float32", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_float32".to_string();
        Some(Arc::new(f.error_to_null().passthrough_nullable()))
    });
}

pub(crate) fn register_decimal_to_int<T: Number>(registry: &mut FunctionRegistry) {
    if T::data_type().is_float() {
        return;
    }
    let name = format!("to_{}", T::data_type().to_string().to_lowercase());
    let try_name = format!("try_to_{}", T::data_type().to_string().to_lowercase());

    let factory = |_params: &[usize], args_type: &[DataType]| {
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

fn convert_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    match from_type {
        DataType::Boolean => boolean_to_decimal(arg, dest_type),
        DataType::Number(ty) => {
            if ty.is_float() {
                float_to_decimal(arg, ctx, *ty, dest_type)
            } else {
                integer_to_decimal(arg, ctx, *ty, dest_type)
            }
        }
        DataType::Decimal(from) => decimal_to_decimal(arg, ctx, *from, dest_type),
        DataType::String => string_to_decimal(arg, ctx, dest_type),
        _ => unreachable!("to_decimal not support this DataType"),
    }
}

fn convert_to_decimal_domain(
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

fn boolean_to_decimal_column<T: Decimal>(
    boolean_column: &Bitmap,
    size: DecimalSize,
) -> DecimalColumn {
    let mut values = Vec::<T>::with_capacity(boolean_column.len());
    for val in boolean_column.iter() {
        if val {
            values.push(T::e(size.scale as u32));
        } else {
            values.push(T::zero());
        }
    }
    T::to_column(values, size)
}

fn boolean_to_decimal_scalar<T: Decimal>(val: bool, size: DecimalSize) -> DecimalScalar {
    if val {
        T::to_scalar(T::e(size.scale as u32), size)
    } else {
        T::to_scalar(T::zero(), size)
    }
}

fn boolean_to_decimal(arg: &ValueRef<AnyType>, dest_type: DecimalDataType) -> Value<AnyType> {
    match arg {
        ValueRef::Column(column) => {
            let boolean_column = BooleanType::try_downcast_column(column).unwrap();
            let column = match dest_type {
                DecimalDataType::Decimal128(size) => {
                    boolean_to_decimal_column::<i128>(&boolean_column, size)
                }
                DecimalDataType::Decimal256(size) => {
                    boolean_to_decimal_column::<i256>(&boolean_column, size)
                }
            };
            Value::Column(Column::Decimal(column))
        }
        ValueRef::Scalar(scalar) => {
            let val = BooleanType::try_downcast_scalar(scalar).unwrap();
            let scalar = match dest_type {
                DecimalDataType::Decimal128(size) => boolean_to_decimal_scalar::<i128>(val, size),
                DecimalDataType::Decimal256(size) => boolean_to_decimal_scalar::<i256>(val, size),
            };
            Value::Scalar(Scalar::Decimal(scalar))
        }
    }
}

fn string_to_decimal_column<T: Decimal>(
    ctx: &mut EvalContext,
    string_column: &StringColumn,
    size: DecimalSize,
    rounding_mode: bool,
) -> DecimalColumn {
    let mut values = Vec::<T>::with_capacity(string_column.len());
    for (row, buf) in string_column.iter().enumerate() {
        match read_decimal_with_size::<T>(buf, size, true, rounding_mode) {
            Ok((d, _)) => values.push(d),
            Err(e) => {
                ctx.set_error(row, e.message());
                values.push(T::zero())
            }
        }
    }
    T::to_column(values, size)
}

fn string_to_decimal_scalar<T: Decimal>(
    ctx: &mut EvalContext,
    string_buf: &[u8],
    size: DecimalSize,
    rounding_mode: bool,
) -> DecimalScalar {
    let value = match read_decimal_with_size::<T>(string_buf, size, true, rounding_mode) {
        Ok((d, _)) => d,
        Err(e) => {
            ctx.set_error(0, e.message());
            T::zero()
        }
    };
    T::to_scalar(value, size)
}

fn string_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let rounding_mode = ctx.func_ctx.rounding_mode;
    match arg {
        ValueRef::Column(column) => {
            let string_column = StringType::try_downcast_column(column).unwrap();
            let column = match dest_type {
                DecimalDataType::Decimal128(size) => {
                    string_to_decimal_column::<i128>(ctx, &string_column, size, rounding_mode)
                }
                DecimalDataType::Decimal256(size) => {
                    string_to_decimal_column::<i256>(ctx, &string_column, size, rounding_mode)
                }
            };
            Value::Column(Column::Decimal(column))
        }
        ValueRef::Scalar(scalar) => {
            let buf = StringType::try_downcast_scalar(scalar).unwrap();
            let scalar = match dest_type {
                DecimalDataType::Decimal128(size) => {
                    string_to_decimal_scalar::<i128>(ctx, buf, size, rounding_mode)
                }
                DecimalDataType::Decimal256(size) => {
                    string_to_decimal_scalar::<i128>(ctx, buf, size, rounding_mode)
                }
            };
            Value::Scalar(Scalar::Decimal(scalar))
        }
    }
}

fn integer_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: NumberDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &DataType::Number(from_type));
            builder.build()
        }
    };

    let result = with_integer_mapped_type!(|NUM_TYPE| match from_type {
        NumberDataType::NUM_TYPE => {
            let column = NumberType::<NUM_TYPE>::try_downcast_column(&column).unwrap();
            integer_to_decimal_internal(column, ctx, &dest_type)
        }
        _ => unreachable!(),
    });

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}

macro_rules! m_integer_to_decimal {
    ($from: expr, $size: expr, $type_name: ty, $ctx: expr) => {
        let multiplier = <$type_name>::e($size.scale as u32);
        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        let values = $from
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = x.as_() * <$type_name>::one();
                let x = x.checked_mul(multiplier).and_then(|v| {
                    if v > max_for_precision || v < min_for_precision {
                        None
                    } else {
                        Some(v)
                    }
                });

                match x {
                    Some(x) => x,
                    None => {
                        $ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                        <$type_name>::one()
                    }
                }
            })
            .collect();
        <$type_name>::to_column(values, $size)
    };
}

fn integer_to_decimal_internal<T: Number + AsPrimitive<i128>>(
    from: Buffer<T>,
    ctx: &mut EvalContext,
    dest_type: &DecimalDataType,
) -> DecimalColumn {
    match dest_type {
        DecimalDataType::Decimal128(size) => {
            m_integer_to_decimal! {from, *size, i128, ctx}
        }
        DecimalDataType::Decimal256(size) => {
            m_integer_to_decimal! {from, *size, i256, ctx}
        }
    }
}

macro_rules! m_float_to_decimal {
    ($from: expr, $size: expr, $type_name: ty, $ctx: expr) => {
        let multiplier: f64 = (10_f64).powi($size.scale as i32).as_();

        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        let values = $from
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let mut x = x.as_() * multiplier;
                if $ctx.func_ctx.rounding_mode {
                    x = x.round();
                }
                let x = <$type_name>::from_float(x);
                if x > max_for_precision || x < min_for_precision {
                    $ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                    <$type_name>::one()
                } else {
                    x
                }
            })
            .collect();
        <$type_name>::to_column(values, $size)
    };
}

fn float_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: NumberDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &DataType::Number(from_type));
            builder.build()
        }
    };

    let result = match from_type {
        NumberDataType::Float32 => {
            let column = NumberType::<F32>::try_downcast_column(&column).unwrap();
            float_to_decimal_internal(column, ctx, &dest_type)
        }
        NumberDataType::Float64 => {
            let column = NumberType::<F64>::try_downcast_column(&column).unwrap();
            float_to_decimal_internal(column, ctx, &dest_type)
        }
        _ => unreachable!(),
    };
    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}

fn float_to_decimal_internal<T: Number + AsPrimitive<f64>>(
    from: Buffer<T>,
    ctx: &mut EvalContext,
    dest_type: &DecimalDataType,
) -> DecimalColumn {
    match dest_type {
        DecimalDataType::Decimal128(size) => {
            m_float_to_decimal! {from, *size, i128, ctx}
        }
        DecimalDataType::Decimal256(size) => {
            m_float_to_decimal! {from, *size, i256, ctx}
        }
    }
}

fn get_round_val<T: Decimal>(x: T, scale: u32, ctx: &mut EvalContext) -> Option<T> {
    let mut round_val = None;
    if ctx.func_ctx.rounding_mode && scale > 0 {
        // Checking whether numbers need to be added or subtracted to calculate rounding
        if let Some(r) = x.checked_rem(T::e(scale)) {
            if let Some(m) = r.checked_div(T::e(scale - 1)) {
                if m >= T::from_i64(5i64) {
                    round_val = Some(T::one());
                } else if m <= T::from_i64(-5i64) {
                    round_val = Some(T::minus_one());
                }
            }
        }
    }
    round_val
}

fn decimal_256_to_128(
    buffer: Buffer<i256>,
    from_size: DecimalSize,
    dest_size: DecimalSize,
    ctx: &mut EvalContext,
) -> DecimalColumn {
    let max = i128::max_for_precision(dest_size.precision);
    let min = i128::min_for_precision(dest_size.precision);

    let values = if dest_size.scale >= from_size.scale {
        let factor = i256::e((dest_size.scale - from_size.scale) as u32);
        buffer
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = x * i128::one();
                match x.checked_mul(factor) {
                    Some(x) if x <= max && x >= min => *x.low(),
                    _ => {
                        ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                        i128::one()
                    }
                }
            })
            .collect()
    } else {
        let scale_diff = (from_size.scale - dest_size.scale) as u32;
        let factor = i256::e(scale_diff);
        let source_factor = i256::e(from_size.scale as u32);

        buffer
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = x * i128::one();
                let round_val = get_round_val::<i256>(x, scale_diff, ctx);
                let y = match (x.checked_div(factor), round_val) {
                    (Some(x), Some(round_val)) => x.checked_add(round_val),
                    (Some(x), None) => Some(x),
                    (None, _) => None,
                };

                match y {
                    Some(y) if (y <= max && y >= min) && (y != 0 || x / source_factor == 0) => {
                        *y.low()
                    }
                    _ => {
                        ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                        i128::one()
                    }
                }
            })
            .collect()
    };
    i128::to_column(values, dest_size)
}

macro_rules! m_decimal_to_decimal {
    ($from_size: expr, $dest_size: expr, $buffer: expr, $from_type_name: ty, $dest_type_name: ty, $ctx: expr) => {
        // faster path
        if $from_size.scale == $dest_size.scale && $from_size.precision <= $dest_size.precision {
            if <$from_type_name>::MAX == <$dest_type_name>::MAX {
                // 128 -> 128 or 256 -> 256
                <$from_type_name>::to_column_from_buffer($buffer, $dest_size)
            } else {
                // 128 -> 256
                let buffer = $buffer
                    .into_iter()
                    .map(|x| x * <$dest_type_name>::one())
                    .collect();
                <$dest_type_name>::to_column(buffer, $dest_size)
            }
        } else {
            let values: Vec<_> = if $from_size.scale > $dest_size.scale {
                let scale_diff = ($from_size.scale - $dest_size.scale) as u32;
                let factor = <$dest_type_name>::e(scale_diff);
                let max = <$dest_type_name>::max_for_precision($dest_size.precision);
                let min = <$dest_type_name>::min_for_precision($dest_size.precision);

                let source_factor = <$from_type_name>::e($from_size.scale as u32);
                $buffer
                    .iter()
                    .enumerate()
                    .map(|(row, x)| {
                        let x = x * <$dest_type_name>::one();
                        let round_val = get_round_val::<$dest_type_name>(x, scale_diff, $ctx);
                        let y = match (x.checked_div(factor), round_val) {
                            (Some(x), Some(round_val)) => x.checked_add(round_val),
                            (Some(x), None) => Some(x),
                            (None, _) => None,
                        };
                        match y {
                            Some(y)
                                if y <= max && y >= min && (y != 0 || x / source_factor == 0) =>
                            {
                                y as $dest_type_name
                            }
                            _ => {
                                $ctx.set_error(
                                    row,
                                    concat!("Decimal overflow at line : ", line!()),
                                );
                                <$dest_type_name>::one()
                            }
                        }
                    })
                    .collect()
            } else {
                let factor = <$dest_type_name>::e(($dest_size.scale - $from_size.scale) as u32);
                let max = <$dest_type_name>::max_for_precision($dest_size.precision);
                let min = <$dest_type_name>::min_for_precision($dest_size.precision);
                $buffer
                    .iter()
                    .enumerate()
                    .map(|(row, x)| {
                        let x = x * <$dest_type_name>::one();
                        match x.checked_mul(factor) {
                            Some(x) if x <= max && x >= min => x as $dest_type_name,
                            _ => {
                                $ctx.set_error(
                                    row,
                                    concat!("Decimal overflow at line : ", line!()),
                                );
                                <$dest_type_name>::one()
                            }
                        }
                    })
                    .collect()
            };
            <$dest_type_name>::to_column(values, $dest_size)
        }
    };
}

fn decimal_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: DecimalDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &DataType::Decimal(from_type));
            builder.build()
        }
    };

    let result: DecimalColumn = match (from_type, dest_type) {
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal128(dest_size)) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, dest_size, buffer, i128, i128, ctx}
        }
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal256(dest_size)) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, dest_size, buffer, i128, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal256(dest_size)) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, dest_size, buffer, i256, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal128(dest_size)) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();
            decimal_256_to_128(buffer, from_size, dest_size, ctx)
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}

fn decimal_to_float64(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    _ctx: &mut EvalContext,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_decimal().unwrap();

    let result = match from_type {
        DecimalDataType::Decimal128(_) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();

            let div = 10_f64.powi(from_size.scale as i32);

            let values: Buffer<F64> = buffer.iter().map(|x| (*x as f64 / div).into()).collect();
            Float64Type::upcast_column(values)
        }

        DecimalDataType::Decimal256(_) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();

            let div = 10_f64.powi(from_size.scale as i32);

            let values: Buffer<F64> = buffer
                .iter()
                .map(|x| (f64::from(*x) / div).into())
                .collect();
            Float64Type::upcast_column(values)
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(scalar.to_owned())
    } else {
        Value::Column(result)
    }
}

fn decimal_to_float32(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    _ctx: &mut EvalContext,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_decimal().unwrap();

    let result = match from_type {
        DecimalDataType::Decimal128(_) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();

            let div = 10_f32.powi(from_size.scale as i32);

            let values: Buffer<F32> = buffer.iter().map(|x| (*x as f32 / div).into()).collect();
            Float32Type::upcast_column(values)
        }

        DecimalDataType::Decimal256(_) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();

            let div = 10_f32.powi(from_size.scale as i32);

            let values: Buffer<F32> = buffer
                .iter()
                .map(|x| (f32::from(*x) / div).into())
                .collect();
            Float32Type::upcast_column(values)
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(scalar.to_owned())
    } else {
        Value::Column(result)
    }
}

fn decimal_to_int<T: Number>(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    ctx: &mut EvalContext,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_decimal().unwrap();

    let result = match from_type {
        DecimalDataType::Decimal128(_) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();

            let mut values = Vec::with_capacity(ctx.num_rows);

            for (i, x) in buffer.iter().enumerate() {
                match x.to_int(from_size.scale, ctx.func_ctx.rounding_mode) {
                    Some(x) => values.push(x),
                    None => {
                        ctx.set_error(i, "decimal cast to int overflow");
                        values.push(T::default())
                    }
                }
            }

            NumberType::<T>::upcast_column(Buffer::from(values))
        }

        DecimalDataType::Decimal256(_) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();
            let mut values = Vec::with_capacity(ctx.num_rows);

            for (i, x) in buffer.iter().enumerate() {
                match x.to_int(from_size.scale, ctx.func_ctx.rounding_mode) {
                    Some(x) => values.push(x),
                    None => {
                        ctx.set_error(i, "decimal cast to int overflow");
                        values.push(T::default())
                    }
                }
            }
            NumberType::<T>::upcast_column(Buffer::from(values))
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(scalar.to_owned())
    } else {
        Value::Column(result)
    }
}
