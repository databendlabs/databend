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

use std::ops::*;
use std::sync::Arc;

use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;

pub fn register_decimal_math(registry: &mut FunctionRegistry) {
    let factory_rounds = |params: &[Scalar], args_type: &[DataType], round_mode: RoundMode| {
        if args_type.is_empty() {
            return None;
        }

        let from_type = args_type[0].remove_nullable();
        if !matches!(from_type, DataType::Decimal(_)) {
            return None;
        }

        let from_size = from_type.as_decimal().unwrap();

        let scale = if params.is_empty() {
            debug_assert!(matches!(round_mode, RoundMode::Ceil | RoundMode::Floor));
            0
        } else {
            params[0].get_i64()?
        };

        let decimal_size = DecimalSize::new_unchecked(
            from_size.precision(),
            scale.clamp(0, from_size.scale() as _) as _,
        );

        let dest_decimal_type = DecimalDataType::from_size(decimal_size).ok()?;
        let name = format!("{:?}", round_mode).to_lowercase();

        let mut sig_args_type = args_type.to_owned();
        sig_args_type[0] = from_type.clone();
        let f = Function {
            signature: FunctionSignature {
                name,
                args_type: sig_args_type,
                return_type: DataType::Decimal(dest_decimal_type.size()),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |_ctx, _d| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| {
                    decimal_rounds(
                        &args[0],
                        ctx,
                        from_type.clone(),
                        dest_decimal_type,
                        scale as _,
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
        registry.register_function_factory(
            &name,
            FunctionFactory::Closure(Box::new(move |params, args_type| {
                Some(Arc::new(factory_rounds(params, args_type, m)?))
            })),
        );
    }

    let factory_abs = |_params: &[Scalar], args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }

        let from_type = args_type[0].remove_nullable();
        if !matches!(from_type, DataType::Decimal(_)) {
            return None;
        }
        let f = Function {
            signature: FunctionSignature {
                name: "abs".to_string(),
                args_type: vec![from_type.clone()],
                return_type: from_type.clone(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |_ctx, _d| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| decimal_abs(&args[0], ctx)),
            },
        };

        if args_type[0].is_nullable() {
            Some(f.passthrough_nullable())
        } else {
            Some(f)
        }
    };

    registry.register_function_factory(
        "abs",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            Some(Arc::new(factory_abs(params, args_type)?))
        })),
    );
}

#[derive(Copy, Clone, Debug)]
enum RoundMode {
    Round,
    Truncate,
    Floor,
    Ceil,
}

fn decimal_round_positive<T>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    T: Decimal + From<i8> + DivAssign + Div<Output = T> + Add<Output = T> + Sub<Output = T>,
{
    let power_of_ten = T::e((source_scale - target_scale) as u32);
    let addition = power_of_ten / T::from(2);
    vectorize_1_arg::<DecimalType<T>, DecimalType<T>>(|a, _| {
        if a < T::zero() {
            (a - addition) / power_of_ten
        } else {
            (a + addition) / power_of_ten
        }
    })(value, ctx)
}

fn decimal_round_negative<T>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
{
    let divide_power_of_ten = T::e((source_scale - target_scale) as u32);
    let addition = divide_power_of_ten / T::from(2);
    let multiply_power_of_ten = T::e((-target_scale) as u32);

    vectorize_1_arg::<DecimalType<T>, DecimalType<T>>(|a, _| {
        let a = if a < T::zero() {
            a - addition
        } else {
            a + addition
        };
        a / divide_power_of_ten * multiply_power_of_ten
    })(value, ctx)
}

// if round mode is ceil, truncate should add one value
fn decimal_truncate_positive<T>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    T: Decimal + From<i8> + DivAssign + Div<Output = T> + Add<Output = T> + Sub<Output = T>,
{
    let power_of_ten = T::e((source_scale - target_scale) as u32);
    vectorize_1_arg::<DecimalType<T>, DecimalType<T>>(|a, _| a / power_of_ten)(value, ctx)
}

fn decimal_truncate_negative<T>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
{
    let divide_power_of_ten = T::e((source_scale - target_scale) as u32);
    let multiply_power_of_ten = T::e((-target_scale) as u32);

    vectorize_1_arg::<DecimalType<T>, DecimalType<T>>(|a, _| {
        a / divide_power_of_ten * multiply_power_of_ten
    })(value, ctx)
}

fn decimal_floor<T>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
{
    let power_of_ten = T::e(source_scale as u32);

    vectorize_1_arg::<DecimalType<T>, DecimalType<T>>(|a, _| {
        if a < T::zero() {
            // below 0 we ceil the number (e.g. -10.5 -> -11)
            ((a + T::one()) / power_of_ten) - T::one()
        } else {
            a / power_of_ten
        }
    })(value, ctx)
}

fn decimal_ceil<T>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
{
    let power_of_ten = T::e(source_scale as u32);

    vectorize_1_arg::<DecimalType<T>, DecimalType<T>>(|a, _| {
        if a <= T::zero() {
            a / power_of_ten
        } else {
            ((a - T::one()) / power_of_ten) + T::one()
        }
    })(value, ctx)
}

fn decimal_rounds(
    arg: &Value<AnyType>,
    ctx: &mut EvalContext,
    from_type: DataType,
    dest_type: DecimalDataType,
    target_scale: i64,
    mode: RoundMode,
) -> Value<AnyType> {
    let size = from_type.as_decimal().unwrap();
    let source_scale = size.scale() as i64;

    if source_scale < target_scale {
        return arg.clone().to_owned();
    }

    let zero_or_positive = target_scale >= 0;

    let (from_decimal_type, _) = DecimalDataType::from_value(arg).unwrap();
    with_decimal_mapped_type!(|TYPE| match from_decimal_type {
        DecimalDataType::TYPE(_) => {
            let value = arg.try_downcast::<DecimalType<TYPE>>().unwrap();

            let result = match (zero_or_positive, mode) {
                (true, RoundMode::Round) => {
                    decimal_round_positive(value, source_scale, target_scale, ctx)
                }
                (true, RoundMode::Truncate) => {
                    decimal_truncate_positive(value, source_scale, target_scale, ctx)
                }
                (false, RoundMode::Round) => {
                    decimal_round_negative(value, source_scale, target_scale, ctx)
                }
                (false, RoundMode::Truncate) => {
                    decimal_truncate_negative(value, source_scale, target_scale, ctx)
                }
                (_, RoundMode::Floor) => decimal_floor(value, source_scale, ctx),
                (_, RoundMode::Ceil) => decimal_ceil(value, source_scale, ctx),
            };

            result.upcast_decimal(dest_type.size())
        }
    })
}

fn decimal_abs(arg: &Value<AnyType>, ctx: &mut EvalContext) -> Value<AnyType> {
    let (data_type, _) = DecimalDataType::from_value(arg).unwrap();
    with_decimal_mapped_type!(|DECIMAL_TYPE| match data_type {
        DecimalDataType::DECIMAL_TYPE(_) => {
            type T = DecimalType<DECIMAL_TYPE>;
            let value = arg.try_downcast::<T>().unwrap();
            let result = vectorize_1_arg::<T, T>(|a, _| a.abs())(value, ctx);
            result.upcast_decimal(data_type.size())
        }
    })
}
