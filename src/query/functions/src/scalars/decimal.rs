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
    ($a: expr, $b: expr, $return_type: expr, $op: tt) => {
        match $return_type {
            DataType::Decimal(d) => match d {
                DecimalDataType::Decimal128(size) => {
                    binary_decimal!($a, $b, $op, *size, i128, Decimal128)
                }
                DecimalDataType::Decimal256(size) => {
                    binary_decimal!($a, $b, $op, *size, i256, Decimal256)
                }
            },
            _ => unreachable!(),
        }
    };
}

macro_rules! binary_decimal {
    ($a: expr, $b: expr, $op: tt, $size: expr, $type_name: ty, $decimal_type: tt) => {
        match ($a, $b) {
            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_b, _))),
            ) => {
                let result: Vec<$type_name> = buffer_a
                    .iter()
                    .zip(buffer_b.iter())
                    .map(|(a, b)| a $op b)
                    .collect();
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(result.into(), $size)))
            }

            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => {
                let result: Vec<$type_name> = buffer.iter().map(|a| a $op b).collect();
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(result.into(), $size)))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
            ) => {
                let result: Vec<$type_name> = buffer.iter().map(|b| a $op b).collect();
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(result.into(), $size)))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => Value::Scalar(Scalar::Decimal(DecimalScalar::$decimal_type(a $op b, $size))),

            _ => unreachable!(),
        }
    };
}

macro_rules! register_decimal_binary_op {
    ($registry: expr, $name: expr, $op: tt) => {
        $registry.register_function_factory($name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            if !args_type[0].is_decimal() || !args_type[1].is_decimal() {
                return None;
            }

            let lhs_type = args_type[0].as_decimal().unwrap();
            let rhs_type = args_type[1].as_decimal().unwrap();

            let is_multiply = $name == "mul";
            let is_divide = $name == "div";
            let is_plus_minus = $name == "plus" || $name == "minus";

            let return_type = DecimalDataType::binary_result_type(
                &lhs_type,
                &rhs_type,
                is_multiply,
                is_divide,
                is_plus_minus,
            )
            .ok()?;

            Some(Arc::new(Function {
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
                eval: Box::new(move |args, _tx| {
                    op_decimal!(
                        &args[0],
                        &args[1],
                        &DataType::Decimal(return_type.clone()),
                        $op
                    )
                }),
            }))
        });
    };
}

// TODO add nullable wrapper
pub fn register(registry: &mut FunctionRegistry) {
    // TODO checked overflow by default
    register_decimal_binary_op!(registry, "plus", +);
    register_decimal_binary_op!(registry, "minus", -);
    register_decimal_binary_op!(registry, "divide", /);
    register_decimal_binary_op!(registry, "multiply", *);

    // int float to decimal
    registry.register_function_factory("to_decimal", |params, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if !args_type[0].is_decimal() && !args_type[0].is_numeric() {
            return None;
        }

        let decimal_size = DecimalSize {
            precision: params[0] as u8,
            scale: params[1] as u8,
        };

        let from_type = args_type[0].clone();
        let return_type = DecimalDataType::from_size(decimal_size).ok()?;
        let return_type = DataType::Decimal(return_type.clone());

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
        todo!()
    }
    todo!()
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
