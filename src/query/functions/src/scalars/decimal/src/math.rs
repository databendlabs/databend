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

use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarFunction;
use databend_common_expression::Value;
use databend_common_expression::types::compute_view::Compute;
use databend_common_expression::types::compute_view::ComputeView;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::with_decimal_mapped_type;

pub fn register_decimal_math(registry: &mut FunctionRegistry) {
    let factory_rounds = |params: &[Scalar], args_type: &[DataType], round_mode: RoundMode| {
        if args_type.is_empty() {
            return None;
        }

        let from_type = args_type[0].remove_nullable();
        if !matches!(from_type, DataType::Decimal(_)) {
            return None;
        }

        let from_size = *from_type.as_decimal().unwrap();

        let scale = if params.is_empty() {
            debug_assert!(matches!(round_mode, RoundMode::Ceil | RoundMode::Floor));
            0
        } else {
            params[0].get_i64()?
        };

        let return_size = DecimalSize::new(
            from_size.precision(),
            scale.clamp(0, from_size.scale() as _) as _,
        )
        .ok()?;

        let name = format!("{:?}", round_mode).to_lowercase();

        let mut sig_args_type = args_type.to_owned();
        sig_args_type[0] = from_type.clone();
        let signature = FunctionSignature {
            name,
            args_type: sig_args_type,
            return_type: DataType::Decimal(return_size),
        };
        let eval = DecimalRound {
            dest_type: DecimalDataType::from(return_size),
            source_scale: from_size.scale() as i64,
            target_scale: scale as i64,
            mode: round_mode,
        };
        Some(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::Full,
            eval,
            None,
            args_type[0].is_nullable(),
        ))
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
        let signature = FunctionSignature {
            name: "abs".to_string(),
            args_type: vec![from_type.clone()],
            return_type: from_type,
        };

        Some(Function::with_passthrough_nullable(
            signature,
            FunctionDomain::Full,
            decimal_abs,
            None,
            args_type[0].is_nullable(),
        ))
    };

    registry.register_function_factory(
        "abs",
        FunctionFactory::Closure(Box::new(move |params, args_type| {
            Some(Arc::new(factory_abs(params, args_type)?))
        })),
    );

    // As in PostgreSQL: the fewest fractional digits that represent the value
    // exactly, `min_scale(1.2300::DECIMAL(10, 4)) = 2`.
    registry.register_function_factory(
        "min_scale",
        FunctionFactory::Closure(Box::new(|_params, args_type| {
            if args_type.len() != 1 {
                return None;
            }
            let from_type = args_type[0].remove_nullable();
            if !matches!(from_type, DataType::Decimal(_)) {
                return None;
            }
            let signature = FunctionSignature {
                name: "min_scale".to_string(),
                args_type: vec![from_type],
                return_type: DataType::Number(NumberDataType::UInt8),
            };
            Some(Arc::new(Function::with_passthrough_nullable(
                signature,
                FunctionDomain::Full,
                decimal_min_scale,
                None,
                args_type[0].is_nullable(),
            )))
        })),
    );
    // A bare NULL has no decimal type for the factory to match.
    registry.register_1_arg_core::<NullType, NullType, _, _>(
        "min_scale",
        |_, _| FunctionDomain::Domain(()),
        |val, _| val.to_owned(),
    );
}

#[derive(Copy, Clone, Debug)]
enum RoundMode {
    Round,
    Truncate,
    Floor,
    Ceil,
}

fn decimal_round_positive<C, T, U>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<U>>
where
    T: Decimal + From<i8> + DivAssign + Div<Output = T> + Add<Output = T> + Sub<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let power_of_ten = T::e((source_scale - target_scale) as u8);
    let addition = power_of_ten / T::from(2);
    vectorize_1_arg::<DecimalType<T>, DecimalType<U>>(|a, _| {
        let res = if a < T::zero() {
            (a - addition) / power_of_ten
        } else {
            (a + addition) / power_of_ten
        };
        C::compute(res)
    })(value, ctx)
}

fn decimal_round_negative<C, T, U>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<U>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let divide_power_of_ten = T::e((source_scale - target_scale) as u8);
    let addition = divide_power_of_ten / T::from(2);
    let multiply_power_of_ten = T::e((-target_scale) as u8);

    vectorize_1_arg::<DecimalType<T>, DecimalType<U>>(|a, _| {
        let a = if a < T::zero() {
            a - addition
        } else {
            a + addition
        };
        let res = a / divide_power_of_ten * multiply_power_of_ten;
        C::compute(res)
    })(value, ctx)
}

// if round mode is ceil, truncate should add one value
fn decimal_truncate_positive<C, T, U>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<U>>
where
    T: Decimal + From<i8> + DivAssign + Div<Output = T> + Add<Output = T> + Sub<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let power_of_ten = T::e((source_scale - target_scale) as u8);
    vectorize_1_arg::<DecimalType<T>, DecimalType<U>>(|a, _| {
        let res = a / power_of_ten;
        C::compute(res)
    })(value, ctx)
}

fn decimal_truncate_negative<C, T, U>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    target_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<U>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let divide_power_of_ten = T::e((source_scale - target_scale) as u8);
    let multiply_power_of_ten = T::e((-target_scale) as u8);

    vectorize_1_arg::<DecimalType<T>, DecimalType<U>>(|a, _| {
        let res = a / divide_power_of_ten * multiply_power_of_ten;
        C::compute(res)
    })(value, ctx)
}

fn decimal_floor<C, T, U>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<U>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let power_of_ten = T::e(source_scale as u8);

    vectorize_1_arg::<DecimalType<T>, DecimalType<U>>(|a, _| {
        let res = if a < T::zero() {
            // below 0 we ceil the number (e.g. -10.5 -> -11)
            ((a + T::one()) / power_of_ten) - T::one()
        } else {
            a / power_of_ten
        };
        C::compute(res)
    })(value, ctx)
}

fn decimal_ceil<C, T, U>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<U>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let power_of_ten = T::e(source_scale as u8);

    vectorize_1_arg::<DecimalType<T>, DecimalType<U>>(|a, _| {
        let res = if a <= T::zero() {
            a / power_of_ten
        } else {
            ((a - T::one()) / power_of_ten) + T::one()
        };
        C::compute(res)
    })(value, ctx)
}

#[derive(Clone)]
struct DecimalRound {
    dest_type: DecimalDataType,
    source_scale: i64,
    target_scale: i64,
    mode: RoundMode,
}

impl ScalarFunction for DecimalRound {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let arg = &args[0];
        let (from_type, _) = DecimalDataType::from_value(arg).unwrap();

        if from_type.data_kind() == self.dest_type.data_kind()
            && self.source_scale < self.target_scale
        {
            return arg.to_owned();
        }

        with_decimal_mapped_type!(|IN| match from_type {
            DecimalDataType::IN(_) => {
                let arg = arg.try_downcast().unwrap();
                with_decimal_mapped_type!(|OUT| match self.dest_type {
                    DecimalDataType::OUT(size) =>
                        decimal_rounds_type::<DecimalConvert<IN, OUT>, _, _>(
                            arg,
                            ctx,
                            self.source_scale,
                            self.target_scale,
                            self.mode
                        )
                        .upcast_with_type(&DataType::Decimal(size)),
                })
            }
        })
    }
}

fn decimal_rounds_type<C, T, U>(
    arg: Value<DecimalType<T>>,
    ctx: &mut EvalContext,
    source_scale: i64,
    target_scale: i64,
    mode: RoundMode,
) -> Value<DecimalType<U>>
where
    T: Decimal
        + From<i8>
        + DivAssign
        + Div<Output = T>
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
{
    let zero_or_positive = target_scale >= 0;
    match (mode, zero_or_positive) {
        (RoundMode::Round, true) => {
            decimal_round_positive::<C, T, U>(arg, source_scale, target_scale, ctx)
        }
        (RoundMode::Round, false) => {
            decimal_round_negative::<C, T, U>(arg, source_scale, target_scale, ctx)
        }

        (RoundMode::Truncate, true) => {
            decimal_truncate_positive::<C, T, U>(arg, source_scale, target_scale, ctx)
        }
        (RoundMode::Truncate, false) => {
            decimal_truncate_negative::<C, T, U>(arg, source_scale, target_scale, ctx)
        }

        (RoundMode::Floor, _) => decimal_floor::<C, T, U>(arg, source_scale, ctx),
        (RoundMode::Ceil, _) => decimal_ceil::<C, T, U>(arg, source_scale, ctx),
    }
}

fn decimal_min_scale(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let arg = &args[0];
    let (from_type, _) = DecimalDataType::from_value(arg).unwrap();

    with_decimal_mapped_type!(|T| match from_type {
        DecimalDataType::T(size) => {
            let value = arg.try_downcast().unwrap();
            vectorize_1_arg::<DecimalType<T>, UInt8Type>(|v, _| min_scale(v, size.scale()))(
                value, ctx,
            )
            .upcast()
        }
    })
}

/// The fractional digits of `value`, an unscaled decimal of `scale`, left once
/// its trailing zeros are dropped. One wide remainder takes the low 18 digits,
/// whose trailing zeros are then counted in a `u64`; only a value whose 18 low
/// digits are all zero, possible at a scale above 18, takes another round.
fn min_scale<T: Decimal>(mut value: T, mut scale: u8) -> u8 {
    const CHUNK: u8 = 18;
    while scale > 0 {
        let digits = scale.min(CHUNK);
        let unit = T::e(digits);
        // `unit` is a positive power of ten, so neither operation can fail.
        let low = value.checked_rem(unit).unwrap();
        if low == T::zero() {
            value = value.checked_div(unit).unwrap();
            scale -= digits;
            continue;
        }
        // Below 10^18 in magnitude, so exact in an i64.
        let mut low = low.as_decimal::<i64>().unsigned_abs();
        let mut zeros = 0;
        while low % 10 == 0 {
            low /= 10;
            zeros += 1;
        }
        return scale - zeros;
    }
    0
}

fn decimal_abs(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let arg: &Value<AnyType> = &args[0];
    let (from_type, _) = DecimalDataType::from_value(arg).unwrap();

    let dest_type = DecimalDataType::from(from_type.size());

    with_decimal_mapped_type!(|IN| match from_type {
        DecimalDataType::IN(size) => {
            with_decimal_mapped_type!(|OUT| match dest_type {
                DecimalDataType::OUT(_) => {
                    let value = arg.try_downcast().unwrap();
                    vectorize_1_arg::<ComputeView<DecimalConvert<IN, OUT>, _, _>, DecimalType<OUT>>(
                        |a, _| a.abs(),
                    )(value, ctx)
                    .upcast_with_type(&DataType::Decimal(size))
                }
            })
        }
    })
}
