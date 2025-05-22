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

use std::ops::Div;
use std::ops::Mul;

use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::i256;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::VariantType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::Value;
use jsonb::RawJsonb;
use num_traits::AsPrimitive;

use crate::cast::get_round_val;

fn json_number_to_decimal<T>(
    value: jsonb::Number,
    dest_type: DecimalDataType,
    max: T,
    min: T,
    multiplier: T,
    multiplier_f64: f64,
    ctx: &mut EvalContext,
) -> Result<T, u32>
where
    T: Decimal + Mul<Output = T> + Div<Output = T>,
{
    let dest_size = dest_type.size();
    match value {
        jsonb::Number::Int64(v) => {
            integer_to_decimal_scalar::<T, NumberType<i64>>(v, dest_size, multiplier)
        }
        jsonb::Number::UInt64(v) => {
            integer_to_decimal_scalar::<T, NumberType<u64>>(v, dest_size, multiplier)
        }
        jsonb::Number::Float64(v) => {
            let mut x = v * multiplier_f64;
            if ctx.func_ctx.rounding_mode {
                x = x.round();
            }
            let x = T::from_float(x);
            if x > max || x < min {
                Err(line!())
            } else {
                Ok(x)
            }
        }
        jsonb::Number::Decimal128(d) => {
            let from_size = DecimalSize::new_unchecked(d.precision, d.scale);
            let x = T::from_i128(d.value);
            decimal_resize::<T>(ctx, x, from_size, dest_size, min, max)
        }
        jsonb::Number::Decimal256(d) => {
            let from_size = DecimalSize::new_unchecked(d.precision, d.scale);
            match dest_type {
                DecimalDataType::Decimal128(_) => {
                    let x = i256(d.value);
                    let min = i256::min_for_precision(dest_size.precision());
                    let max = i256::max_for_precision(dest_size.precision());
                    decimal_resize(ctx, x, from_size, dest_size, min, max).map(T::from_i256)
                }
                DecimalDataType::Decimal256(_) => {
                    let x = T::from_i256(i256(d.value));
                    decimal_resize::<T>(ctx, x, from_size, dest_size, min, max)
                }
            }
        }
    }
}

pub(super) fn variant_to_decimal<T>(
    from: Value<VariantType>,
    ctx: &mut EvalContext,
    dest_type: DecimalDataType,
) -> Value<DecimalType<T>>
where
    T: Decimal + Mul<Output = T> + Div<Output = T>,
{
    let size = dest_type.size();
    let multiplier = T::e(size.scale() as u32);
    let multiplier_f64: f64 = (10_f64).powi(size.scale() as i32).as_();
    let min = T::min_for_precision(size.precision());
    let max = T::max_for_precision(size.precision());
    let f = |val: &[u8], builder: &mut Vec<T>, ctx: &mut EvalContext| {
        let raw_jsonb = RawJsonb::new(val);
        if let Ok(Some(b)) = raw_jsonb.as_bool() {
            let value = if b {
                T::e(size.scale() as u32)
            } else {
                T::zero()
            };
            builder.push(value);
            return;
        }
        if let Ok(Some(x)) = raw_jsonb.as_str() {
            let value = match read_decimal_with_size::<T>(
                x.as_bytes(),
                size,
                true,
                ctx.func_ctx.rounding_mode,
            ) {
                Ok((d, _)) => d,
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    T::zero()
                }
            };
            builder.push(value);
            return;
        }
        let value = match raw_jsonb
            .as_number()
            .map_err(|e| format!("{e}"))
            .and_then(|r| r.ok_or("invalid json type".to_string()))
            .and_then(|v| {
                json_number_to_decimal(v, dest_type, max, min, multiplier, multiplier_f64, ctx)
                    .map_err(|line| format!("decimal overflow at line: {line})"))
            }) {
            Ok(v) => v,
            Err(e) => {
                ctx.set_error(builder.len(), e);
                T::one()
            }
        };
        builder.push(value);
    };

    vectorize_with_builder_1_arg::<VariantType, DecimalType<T>>(f)(from, ctx)
}

fn integer_to_decimal_scalar<T, S>(
    x: S::ScalarRef<'_>,
    size: DecimalSize,
    multiplier: T,
) -> Result<T, u32>
where
    T: Decimal + Mul<Output = T>,
    S: ArgType,
    for<'a> S::ScalarRef<'a>: Number + AsPrimitive<i128>,
{
    let min_for_precision = T::min_for_precision(size.precision());
    let max_for_precision = T::max_for_precision(size.precision());
    if let Some(x) = T::from_i128(x.as_()).checked_mul(multiplier) {
        if x > max_for_precision || x < min_for_precision {
            Err(line!())
        } else {
            Ok(x)
        }
    } else {
        Err(line!())
    }
}

fn decimal_resize<D>(
    ctx: &mut EvalContext,
    x: D,
    from_size: DecimalSize,
    dest_size: DecimalSize,
    min: D,
    max: D,
) -> Result<D, u32>
where
    D: Decimal + Div<Output = D>,
{
    if from_size.scale() == dest_size.scale() && from_size.precision() <= dest_size.precision() {
        Ok(x)
    } else if from_size.scale() > dest_size.scale() {
        let scale_diff = (from_size.scale() - dest_size.scale()) as u32;
        let factor = D::e(scale_diff);
        let source_factor = D::e(from_size.scale() as u32);

        let round_val = get_round_val::<D>(x, scale_diff, ctx);
        let y = match (x.checked_div(factor), round_val) {
            (Some(x), Some(round_val)) => x.checked_add(round_val),
            (Some(x), None) => Some(x),
            (None, _) => None,
        };

        match y {
            Some(y)
                if y <= max && y >= min && (y != D::zero() || x / source_factor == D::zero()) =>
            {
                Ok(y)
            }
            _ => Err(line!()),
        }
    } else {
        let factor = D::e((dest_size.scale() - from_size.scale()) as u32);
        match x.checked_mul(factor) {
            Some(y) if y <= max && y >= min => Ok(y),
            _ => Err(line!()),
        }
    }
}
