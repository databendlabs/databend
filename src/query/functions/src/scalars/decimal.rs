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

use common_expression::types::decimal::*;
use common_expression::types::*;
use common_expression::wrap_nullable;
use common_expression::Column;
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

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("plus", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }

        if !args_type[0].is_decimal() || !args_type[1].is_decimal() {
            return None;
        }

        let lhs_type = args_type[0].as_decimal().unwrap();
        let rhs_type = args_type[1].as_decimal().unwrap();

        let return_type =
            DecimalDataType::binary_result_type(&lhs_type, &rhs_type, false, false, true).ok()?;

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "plus".to_string(),
                args_type: vec![
                    DataType::Decimal(return_type.clone()),
                    DataType::Decimal(return_type.clone()),
                ],
                return_type: DataType::Decimal(return_type.clone()),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let _domain = args_domain[0].as_string().unwrap();
                FunctionDomain::Full
            }),
            eval: Box::new(move |args, _tx| {
                op_decimal!(&args[0], &args[1], &DataType::Decimal(return_type.clone()), +)
            }),
        }))
    });
}
