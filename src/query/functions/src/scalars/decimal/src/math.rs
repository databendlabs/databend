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

use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::PartialEvalPolicy;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarFunction;
use databend_common_expression::Value;
use databend_common_expression::types::compute_view::ComputeView;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_1_arg_with_policy;
use databend_common_expression::with_decimal_mapped_type;

use crate::decimal::DecimalExt;
use crate::vectorize::skip_inactive_with_builder_1_arg;

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
            if matches!(round_mode, RoundMode::Round)
                && from_size.scale() == 0
                && scale < 0
                && scale.unsigned_abs() <= from_size.precision() as u64
            {
                FunctionDomain::MayThrow
            } else {
                FunctionDomain::Full
            },
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

// Every rounding kernel divides by a runtime power of ten, which dominates a
// per-row validity test, so masked rows are skipped. Unmasked batches keep the
// framework's plain loop.
fn map_active<T: DecimalExt>(
    value: Value<DecimalType<T>>,
    ctx: &mut EvalContext,
    f: impl Fn(T) -> T + Copy + Send + Sync,
) -> Value<DecimalType<T>> {
    vectorize_1_arg_with_policy::<DecimalType<T>, DecimalType<T>>(
        PartialEvalPolicy::SkipInactiveRows,
        move |a, _| f(a),
    )(value, ctx)
}

fn decimal_floor<T: DecimalExt>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>> {
    let power_of_ten = T::e(source_scale as u8);
    map_active(value, ctx, move |a| {
        if a < T::zero() {
            // below 0 we ceil the number (e.g. -10.5 -> -11)
            ((a + T::one()) / power_of_ten) - T::one()
        } else {
            a / power_of_ten
        }
    })
}

fn decimal_ceil<T: DecimalExt>(
    value: Value<DecimalType<T>>,
    source_scale: i64,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>> {
    let power_of_ten = T::e(source_scale as u8);
    map_active(value, ctx, move |a| {
        if a <= T::zero() {
            a / power_of_ten
        } else {
            ((a - T::one()) / power_of_ten) + T::one()
        }
    })
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
        // The result keeps the input precision, hence its storage.
        if self.source_scale <= self.target_scale {
            return args[0].to_owned();
        }
        with_decimal_mapped_type!(|T| match self.dest_type {
            DecimalDataType::T(size) => decimal_rounds_type::<T>(
                args[0].try_downcast().unwrap(),
                ctx,
                self.source_scale,
                self.target_scale,
                size.precision(),
                self.mode,
            )
            .upcast_with_type(&DataType::Decimal(size)),
        })
    }
}

fn decimal_rounds_type<T: DecimalExt>(
    arg: Value<DecimalType<T>>,
    ctx: &mut EvalContext,
    source_scale: i64,
    target_scale: i64,
    precision: u8,
    mode: RoundMode,
) -> Value<DecimalType<T>> {
    // Callers pass `target_scale < source_scale`.
    match mode {
        RoundMode::Round | RoundMode::Truncate => {
            // Bound the exponent once. An excessive negative scale rounds to
            // zero.
            let negative = if target_scale < 0 {
                target_scale.unsigned_abs()
            } else {
                0
            };
            if negative > precision as u64 {
                return map_active(arg, ctx, |_| T::zero());
            }
            let shift = if target_scale < 0 {
                source_scale as u64 + negative
            } else {
                (source_scale - target_scale) as u64
            };
            if shift > precision as u64 {
                return map_active(arg, ctx, |_| T::zero());
            }
            let divisor = T::e(shift as u8);
            if matches!(mode, RoundMode::Truncate) {
                if negative == 0 {
                    return map_active(arg, ctx, move |a| a / divisor);
                }
                let multiplier = T::e(negative as u8);
                return map_active(arg, ctx, move |a| a / divisor * multiplier);
            }
            let half = divisor / T::from(2);
            if negative == 0 {
                return map_active(arg, ctx, move |a| {
                    if a < T::zero() {
                        (a - half) / divisor
                    } else {
                        (a + half) / divisor
                    }
                });
            }
            let multiplier = T::e(negative as u8);
            if source_scale > 0 {
                // Dropping fractional digits leaves room for the carry.
                return map_active(arg, ctx, move |a| {
                    let a = if a < T::zero() { a - half } else { a + half };
                    a / divisor * multiplier
                });
            }
            // An integral value rounded to tens or coarser can carry past p.
            // Native arithmetic fits by the declared precision.
            let min = T::min_for_precision(precision);
            let max = T::max_for_precision(precision);
            skip_inactive_with_builder_1_arg::<DecimalType<T>, DecimalType<T>>(
                move |value, out: &mut Vec<T>, ctx| {
                    let value = if value < T::zero() {
                        value - half
                    } else {
                        value + half
                    };
                    let value = value / divisor * multiplier;
                    if value < min || value > max {
                        ctx.set_error(out.len(), "Decimal round overflow");
                        out.push(T::one());
                    } else {
                        out.push(value);
                    }
                },
            )(arg, ctx)
        }
        RoundMode::Floor => decimal_floor(arg, source_scale, ctx),
        RoundMode::Ceil => decimal_ceil(arg, source_scale, ctx),
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

#[cfg(test)]
mod tests {
    use databend_common_expression::FunctionRegistry;
    use databend_common_expression::types::NumberScalar;
    use num_bigint::BigInt;
    use proptest::prelude::*;

    use super::*;
    use crate::testing::*;

    fn registry() -> FunctionRegistry {
        let mut registry = FunctionRegistry::empty();
        register_decimal_math(&mut registry);
        registry
    }

    /// Exact result: round halves away from zero, truncate toward zero, floor
    /// and ceil to integers. A target below zero rounds to tens or coarser and
    /// can carry past the precision.
    fn expected(name: &str, size: DecimalSize, target: Option<i64>, value: &BigInt) -> Expected {
        let source = size.scale() as i64;
        let target = target.unwrap_or(0);
        if target >= source {
            return Some(value.clone());
        }
        let shift = source.abs_diff(target);
        let precision = size.precision() as u64;
        if shift > precision + 1 {
            // |value| < 10^p is below a tenth of the unit.
            return Some(BigInt::from(0));
        }
        let unit = pow10(shift as u32);
        let truncated = value / &unit;
        let inexact = value % &unit != BigInt::from(0);
        let quotient = match name {
            "round" => round_half_away(value, &unit),
            "truncate" => truncated,
            "floor" if inexact && value < &BigInt::from(0) => truncated - 1,
            "ceil" if inexact && value > &BigInt::from(0) => truncated + 1,
            _ => truncated,
        };
        let result = if target < 0 && quotient != BigInt::from(0) {
            quotient * pow10(target.unsigned_abs() as u32)
        } else {
            quotient
        };
        fits(&result, size.precision()).then_some(result)
    }

    fn check(
        registry: &FunctionRegistry,
        name: &str,
        ty: &DataType,
        target: Option<i64>,
        values: &[BigInt],
        visible: Option<&[bool]>,
    ) {
        let size = *ty.as_decimal().unwrap();
        let params = target
            .map(|target| vec![Scalar::Number(NumberScalar::Int64(target))])
            .unwrap_or_default();
        let function = bind_function(registry, name, params, std::slice::from_ref(ty));
        let payloads = hidden_payloads(ty);
        let values = (0..values.len())
            .map(|row| match visible {
                Some(visible) if !visible[row] => payloads[row % 2].clone(),
                _ => values[row].clone(),
            })
            .collect::<Vec<_>>();
        let expected = values
            .iter()
            .map(|value| expected(name, size, target, value))
            .collect::<Vec<_>>();
        check_rows(
            &function,
            &[Value::Column(column(ty, &values))],
            visible,
            &expected,
            |row| format!("{name}({target:?})({ty}) row {row}: {}", values[row]),
        );
    }

    fn targets(size: DecimalSize) -> Vec<i64> {
        let (p, s) = (size.precision() as i64, size.scale() as i64);
        let mut targets = (-p - 2..=s + 1).collect::<Vec<_>>();
        targets.extend([i64::MIN, i64::MAX]);
        targets
    }

    // Every decimal shape and target scale over edge values, once without
    // validity and once with every third row hidden behind storage extremes.
    #[test]
    fn rounding_matches_bigint_on_edges() {
        let registry = registry();
        for p in [1, 2, 5, 18, 19, 38, 39, 76] {
            for s in scales(p, [0, 1, p / 2, p - 1, p]) {
                let size = DecimalSize::new(p, s).unwrap();
                let ty = DataType::Decimal(size);
                let values = edges(&ty);
                let visible = (0..values.len())
                    .map(|row| row % 3 != 0)
                    .collect::<Vec<_>>();
                for visible in [None, Some(&visible[..])] {
                    for name in ["round", "truncate"] {
                        for target in targets(size) {
                            check(&registry, name, &ty, Some(target), &values, visible);
                        }
                    }
                    for name in ["floor", "ceil"] {
                        check(&registry, name, &ty, None, &values, visible);
                    }
                }
            }
        }
    }

    proptest! {
        #[test]
        fn rounding_matches_bigint(
            p in 1_u8..=76,
            s in any::<u8>(),
            target in -80_i64..=80,
            seeds in any::<[[u8; 32]; 4]>(),
            visible in any::<[bool; 4]>(),
            function in 0..4_usize,
        ) {
            let size = DecimalSize::new(p, s % (p + 1)).unwrap();
            let ty = DataType::Decimal(size);
            let max = pow10(p as u32) - 1;
            let values = seeds
                .iter()
                .map(|seed| BigInt::from_signed_bytes_le(seed) % (&max + 1))
                .collect::<Vec<_>>();
            let name = ["round", "truncate", "floor", "ceil"][function];
            let target = (function < 2).then_some(target);
            check(&registry(), name, &ty, target, &values, Some(&visible));
        }
    }
}
