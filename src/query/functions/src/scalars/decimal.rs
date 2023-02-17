// Copyright 2022 Datafuse Labs.
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

use std::ops::*;
use std::sync::Arc;

use common_arrow::arrow::buffer::Buffer;
use common_expression::types::decimal::*;
use common_expression::types::*;
use common_expression::with_integer_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;
use ethnum::i256;
use num_traits::AsPrimitive;

macro_rules! op_decimal {
    ($a: expr, $b: expr, $ctx: expr, $return_type: expr, $op: ident, $scale_a: expr, $scale_b: expr) => {
        match $return_type {
            DataType::Decimal(d) => match d {
                DecimalDataType::Decimal128(size) => {
                    binary_decimal!(
                        $a, $b, $ctx, $op, *size, $scale_a, $scale_b, i128, Decimal128
                    )
                }
                DecimalDataType::Decimal256(size) => {
                    binary_decimal!(
                        $a, $b, $ctx, $op, *size, $scale_a, $scale_b, i256, Decimal256
                    )
                }
            },
            _ => unreachable!(),
        }
    };
}

macro_rules! binary_decimal {
    ($a: expr, $b: expr, $ctx: expr, $op: ident, $size: expr, $scale_a: expr, $scale_b: expr, $type_name: ty, $decimal_type: tt) => {{
        let scale_a = <$type_name>::e($scale_a);
        let scale_b = <$type_name>::e($scale_b);

        let one = <$type_name>::one();
        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        match ($a, $b) {
            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_b, _))),
            ) => {
                let mut result = Vec::with_capacity(buffer_a.len());

                for (a, b) in buffer_a.iter().zip(buffer_b.iter()) {
                    let t = (a * scale_a).$op(b) / scale_b;
                    if t < min_for_precision || t > max_for_precision {
                        $ctx.set_error(result.len(), "Decimal overflow");
                        result.push(one);
                    } else {
                        result.push(t);
                    }
                }
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(
                    result.into(),
                    $size,
                )))
            }

            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => {
                let mut result = Vec::with_capacity(buffer.len());

                for a in buffer.iter() {
                    let t = (a * scale_a).$op(b) / scale_b;
                    if t < min_for_precision || t > max_for_precision {
                        $ctx.set_error(result.len(), "Decimal overflow");
                        result.push(one);
                    } else {
                        result.push(t);
                    }
                }

                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(
                    result.into(),
                    $size,
                )))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
            ) => {
                let mut result = Vec::with_capacity(buffer.len());

                for b in buffer.iter() {
                    let t = (a * scale_a).$op(b) / scale_b;
                    if t < min_for_precision || t > max_for_precision {
                        $ctx.set_error(result.len(), "Decimal overflow");
                        result.push(one);
                    } else {
                        result.push(t);
                    }
                }
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(
                    result.into(),
                    $size,
                )))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => {
                let t = (a * scale_a).$op(b) / scale_b;
                if t < min_for_precision || t > max_for_precision {
                    $ctx.set_error(0, "Decimal overflow");
                } else {
                }
                Value::Scalar(Scalar::Decimal(DecimalScalar::$decimal_type(t, $size)))
            }

            _ => unreachable!(),
        }
    }};
}

macro_rules! register_decimal_binary_op {
    ($registry: expr, $name: expr, $op: ident) => {
        $registry.register_function_factory($name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
            let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

            // number X decimal -> decimal
            // decimal X number -> decimal
            // decimal X decimal -> decimal
            if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
                return None;
            }

            let is_multiply = $name == "multiply";
            let is_divide = $name == "divide";
            let is_plus_minus = $name == "plus" || $name == "minus";

            let return_type = if args_type[0].is_decimal() && args_type[1].is_decimal() {
                let lhs_type = args_type[0].as_decimal().unwrap();
                let rhs_type = args_type[1].as_decimal().unwrap();

                DecimalDataType::binary_result_type(
                    &lhs_type,
                    &rhs_type,
                    is_multiply,
                    is_divide,
                    is_plus_minus,
                )
            } else if args_type[0].is_decimal() {
                let lhs_type = args_type[0].as_decimal().unwrap();
                lhs_type.binary_upgrade_to_max_precision()
            } else {
                let rhs_type = args_type[1].as_decimal().unwrap();
                rhs_type.binary_upgrade_to_max_precision()
            }
            .ok()?;

            let mut scale_a = 0;
            let mut scale_b = 0;

            if is_multiply {
                scale_b = return_type.scale() as u32;
            } else if is_divide {
                scale_a = return_type.scale() as u32;
            }

            let function = Function {
                signature: FunctionSignature {
                    name: $name.to_string(),
                    args_type: vec![
                        DataType::Decimal(return_type.clone()),
                        DataType::Decimal(return_type.clone()),
                    ],
                    return_type: DataType::Decimal(return_type.clone()),
                    property: FunctionProperty::default(),
                },
                calc_domain: Box::new(|_args_domain| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| {
                    op_decimal!(
                        &args[0],
                        &args[1],
                        ctx,
                        &DataType::Decimal(return_type.clone()),
                        $op,
                        scale_a,
                        scale_b
                    )
                }),
            };
            if has_nullable {
                Some(Arc::new(function.wrap_nullable()))
            } else {
                Some(Arc::new(function))
            }
        });
    };
}

pub fn register(registry: &mut FunctionRegistry) {
    // TODO checked overflow by default
    register_decimal_binary_op!(registry, "plus", add);
    register_decimal_binary_op!(registry, "minus", sub);
    register_decimal_binary_op!(registry, "divide", div);
    register_decimal_binary_op!(registry, "multiply", mul);

    // int float to decimal
    registry.register_function_factory("to_decimal", |params, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if !args_type[0].is_decimal() && !args_type[0].is_numeric() {
            return None;
        }

        if params.len() != 2 {
            return None;
        }

        let decimal_size = DecimalSize {
            precision: params[0] as u8,
            scale: params[1] as u8,
        };

        let from_type = args_type[0].clone();
        let return_type = DecimalDataType::from_size(decimal_size).ok()?;
        let return_type = DataType::Decimal(return_type);

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_decimal".to_string(),
                args_type: args_type.to_owned(),
                return_type: return_type.clone(),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|_args_domain| FunctionDomain::Full),
            eval: Box::new(move |args, tx| {
                convert_to_decimal(args, tx, from_type.clone(), return_type.clone())
            }),
        }))
    });
}

fn convert_to_decimal(
    args: &[ValueRef<AnyType>],
    ctx: &mut EvalContext,
    from_type: DataType,
    dest_type: DataType,
) -> Value<AnyType> {
    let arg = &args[0];
    if from_type.is_integer() {
        return integer_to_decimal(arg, ctx, from_type, dest_type);
    }
    if from_type.is_floating() {
        return float_to_decimal(arg, ctx, from_type, dest_type);
    }
    decimal_to_decimal(arg, ctx, from_type, dest_type)
}

fn integer_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: DataType,
    dest_type: DataType,
) -> Value<AnyType> {
    let dest_type = dest_type.as_decimal().unwrap();

    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_number().unwrap();
    let result = with_integer_mapped_type!(|NUM_TYPE| match from_type {
        NumberDataType::NUM_TYPE => {
            let column = NumberType::<NUM_TYPE>::try_downcast_column(&column).unwrap();
            integer_to_decimal_internal(column, ctx, dest_type)
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
                        $ctx.set_error(row, "Decimal overflow");
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
                let x = <$type_name>::from_float(x.as_() * multiplier);
                if x > max_for_precision || x < min_for_precision {
                    $ctx.set_error(row, "Decimal overflow");
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
    from_type: DataType,
    dest_type: DataType,
) -> Value<AnyType> {
    let dest_type = dest_type.as_decimal().unwrap();

    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_number().unwrap();
    let result = match from_type {
        NumberDataType::Float32 => {
            let column = NumberType::<F32>::try_downcast_column(&column).unwrap();
            float_to_decimal_internal(column, ctx, dest_type)
        }
        NumberDataType::Float64 => {
            let column = NumberType::<F64>::try_downcast_column(&column).unwrap();
            float_to_decimal_internal(column, ctx, dest_type)
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

macro_rules! m_decimal_to_decimal {
    ($from_size: expr, $dest_size: expr, $buffer: expr, $from_type_name: ty, $dest_type_name: ty, $ctx: expr) => {
        // faster path
        if $from_size.scale == $dest_size.scale && $from_size.precision <= $dest_size.precision {
            <$from_type_name>::to_column_from_buffer($buffer, $dest_size)
        } else {
            let values = if $from_size.scale > $dest_size.scale {
                let factor = <$dest_type_name>::e(($from_size.scale - $dest_size.scale) as u32);
                $buffer
                    .iter()
                    .enumerate()
                    .map(|(row, x)| {
                        let x = x * <$dest_type_name>::one();
                        match x.checked_div(factor) {
                            Some(x) => x,
                            None => {
                                $ctx.set_error(row, "Decimal overflow");
                                <$dest_type_name>::one()
                            }
                        }
                    })
                    .collect()
            } else {
                let factor = <$dest_type_name>::e(($dest_size.scale - $from_size.scale) as u32);
                $buffer
                    .iter()
                    .enumerate()
                    .map(|(row, x)| {
                        let x = x * <$dest_type_name>::one();
                        match x.checked_mul(factor) {
                            Some(x) => x,
                            None => {
                                $ctx.set_error(row, "Decimal overflow");
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
    from_type: DataType,
    dest_type: DataType,
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
    let dest_type = dest_type.as_decimal().unwrap();

    let result: DecimalColumn = match (from_type, dest_type) {
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal128(dest_size)) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, *dest_size, buffer, i128, i128, ctx}
        }
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal256(dest_size)) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, *dest_size, buffer, i128, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal256(dest_size)) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, *dest_size, buffer, i256, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal128(_)) => {
            unreachable!("Decimal256 to Decimal128 path is unreachable")
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}
