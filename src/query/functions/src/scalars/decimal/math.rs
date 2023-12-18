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

use std::cmp::Ord;
use std::ops::*;
use std::sync::Arc;

use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use ethnum::i256;

pub fn register_decimal_math(registry: &mut FunctionRegistry) {
    let factory = |params: &[usize], args_type: &[DataType], round_mode: RoundMode| {
        if args_type.is_empty() {
            return None;
        }

        let from_type = args_type[0].remove_nullable();
        if !matches!(from_type, DataType::Decimal(_)) {
            return None;
        }

        let from_decimal_type = from_type.as_decimal().unwrap();

        let scale = if params.is_empty() {
            0
        } else {
            params[0] as i64 - 76
        };

        let decimal_size = DecimalSize {
            precision: from_decimal_type.precision(),
            scale: scale.clamp(0, from_decimal_type.scale() as i64) as u8,
        };

        let dest_decimal_type = DecimalDataType::from_size(decimal_size).ok()?;
        let name = format!("{:?}", round_mode).to_lowercase();

        let mut sig_args_type = args_type.to_owned();
        sig_args_type[0] = from_type.clone();
        let f = Function {
            signature: FunctionSignature {
                name,
                args_type: sig_args_type,
                return_type: DataType::Decimal(dest_decimal_type),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |_ctx, _d| FunctionDomain::Full),
                eval: Box::new(move |args, _ctx| {
                    decimal_math(
                        &args[0],
                        from_type.clone(),
                        dest_decimal_type,
                        scale,
                        round_mode,
                    )
                }),
            },
        };

        if args_type[0].is_nullable() {
            Some(f.passthrough_nullable())
        } else {
            Some(f)
        }
    };

    for m in [
        RoundMode::Round,
        RoundMode::Truncate,
        RoundMode::Ceil,
        RoundMode::Floor,
    ] {
        let name = format!("{:?}", m).to_lowercase();
        registry.register_function_factory(&name, move |params, args_type| {
            Some(Arc::new(factory(params, args_type, m)?))
        });
    }
}

#[derive(Copy, Clone, Debug)]
enum RoundMode {
    Round,
    Truncate,
    Floor,
    Ceil,
}

fn decimal_round_positive<T>(values: &[T], source_scale: i64, target_scale: i64) -> Vec<T>
where T: Decimal + From<i8> + DivAssign + Div<Output = T> + Add<Output = T> + Sub<Output = T> {
    let power_of_ten = T::e((source_scale - target_scale) as u32);
    let addition = power_of_ten / T::from(2);

    values
        .iter()
        .map(|input| {
            let input = if input < &T::zero() {
                *input - addition
            } else {
                *input + addition
            };
            input / power_of_ten
        })
        .collect()
}

fn decimal_round_negative<T>(values: &[T], source_scale: i64, target_scale: i64) -> Vec<T>
where T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T> {
    let divide_power_of_ten = T::e((source_scale - target_scale) as u32);
    let addition = divide_power_of_ten / T::from(2);
    let multiply_power_of_ten = T::e((-target_scale) as u32);

    values
        .iter()
        .map(|input| {
            let input = if input < &T::zero() {
                *input - addition
            } else {
                *input + addition
            };
            input / divide_power_of_ten * multiply_power_of_ten
        })
        .collect()
}

// if round mode is ceil, truncate should add one value
fn decimal_truncate_positive<T>(values: &[T], source_scale: i64, target_scale: i64) -> Vec<T>
where T: Decimal + From<i8> + DivAssign + Div<Output = T> + Add<Output = T> + Sub<Output = T> {
    let power_of_ten = T::e((source_scale - target_scale) as u32);

    values.iter().map(|input| *input / power_of_ten).collect()
}

fn decimal_truncate_negative<T>(values: &[T], source_scale: i64, target_scale: i64) -> Vec<T>
where T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T> {
    let divide_power_of_ten = T::e((source_scale - target_scale) as u32);
    let multiply_power_of_ten = T::e((-target_scale) as u32);

    values
        .iter()
        .map(|input| *input / divide_power_of_ten * multiply_power_of_ten)
        .collect()
}

fn decimal_floor<T>(values: &[T], source_scale: i64) -> Vec<T>
where T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T> {
    let power_of_ten = T::e(source_scale as u32);

    values
        .iter()
        .map(|input| {
            if input < &T::zero() {
                // below 0 we ceil the number (e.g. -10.5 -> -11)
                ((*input + T::one()) / power_of_ten) - T::one()
            } else {
                *input / power_of_ten
            }
        })
        .collect()
}

fn decimal_ceil<T>(values: &[T], source_scale: i64) -> Vec<T>
where T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T> {
    let power_of_ten = T::e(source_scale as u32);

    values
        .iter()
        .map(|input| {
            if input <= &T::zero() {
                *input / power_of_ten
            } else {
                ((*input - T::one()) / power_of_ten) + T::one()
            }
        })
        .collect()
}

fn decimal_math(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    dest_type: DecimalDataType,
    target_scale: i64,
    mode: RoundMode,
) -> Value<AnyType> {
    let from_decimal_type = from_type.as_decimal().unwrap();
    let source_scale = from_decimal_type.scale() as i64;

    if source_scale < target_scale {
        return arg.clone().to_owned();
    }

    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let none_negative = target_scale >= 0;

    let result = with_decimal_mapped_type!(|DECIMAL_TYPE| match from_decimal_type {
        DecimalDataType::DECIMAL_TYPE(_) => {
            let (buffer, _) = DECIMAL_TYPE::try_downcast_column(&column).unwrap();

            let result = match (none_negative, mode) {
                (true, RoundMode::Round) => {
                    decimal_round_positive::<_>(&buffer, source_scale, target_scale)
                }
                (true, RoundMode::Truncate) => {
                    decimal_truncate_positive::<_>(&buffer, source_scale, target_scale)
                }
                (false, RoundMode::Round) => {
                    decimal_round_negative::<_>(&buffer, source_scale, target_scale)
                }
                (false, RoundMode::Truncate) => {
                    decimal_truncate_negative::<_>(&buffer, source_scale, target_scale)
                }
                (_, RoundMode::Floor) => decimal_floor::<_>(&buffer, source_scale),
                (_, RoundMode::Ceil) => decimal_ceil::<_>(&buffer, source_scale),
            };
            DECIMAL_TYPE::to_column(result, dest_type.size())
        }
    });

    let result = Column::Decimal(result);
    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(scalar.to_owned())
    } else {
        Value::Column(result)
    }
}
