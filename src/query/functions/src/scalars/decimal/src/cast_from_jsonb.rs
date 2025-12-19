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

use databend_common_expression::EvalContext;
use databend_common_expression::Value;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::F64;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::Number;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use jsonb::Number as JsonbNumber;
use jsonb::Value as JsonbValue;
use num_traits::AsPrimitive;

use crate::decimal_scale_reduction;

pub(super) fn variant_to_decimal<T>(
    from: Value<VariantType>,
    ctx: &mut EvalContext,
    dest_type: DecimalDataType,
    only_cast_number: bool,
) -> Value<NullableType<DecimalType<T>>>
where
    T: Decimal,
{
    let size = dest_type.size();
    let multiplier = T::e(size.scale());
    let multiplier_f64: f64 = (10_f64).powi(size.scale() as i32).as_();
    let min = T::min_for_precision(size.precision());
    let max = T::max_for_precision(size.precision());

    let is_scalar = from.as_scalar().is_some();
    let len = if is_scalar { 1 } else { ctx.num_rows };
    let mut builder = NullableColumnBuilder {
        builder: Vec::with_capacity(len),
        validity: MutableBitmap::with_capacity(len),
    };

    for index in 0..len {
        if let Some(validity) = &ctx.validity {
            if !validity.get_bit(index) {
                builder.push_null();
                continue;
            }
        }
        let val = unsafe { from.index_unchecked(index) };
        match cast_to_decimal(
            val,
            min,
            max,
            multiplier,
            multiplier_f64,
            size,
            dest_type,
            ctx.func_ctx.rounding_mode,
            only_cast_number,
        ) {
            Ok(Some(val)) => builder.push(val),
            Ok(None) => builder.push_null(),
            Err(err) => {
                ctx.set_error(builder.len(), err);
                builder.push_null();
            }
        }
    }
    if is_scalar {
        Value::Scalar(builder.build_scalar())
    } else {
        Value::Column(builder.build())
    }
}

fn cast_to_decimal<T>(
    val: &[u8],
    min: T,
    max: T,
    multiplier: T,
    multiplier_f64: f64,
    dest_size: DecimalSize,
    dest_type: DecimalDataType,
    rounding_mode: bool,
    only_cast_number: bool,
) -> Result<Option<T>, String>
where
    T: Decimal,
{
    let value = jsonb::from_slice(val).map_err(|e| format!("Invalid jsonb value, {e}"))?;
    match value {
        JsonbValue::Number(num) => match num {
            JsonbNumber::Int64(n) => {
                match T::from_i64(n)
                    .checked_mul(multiplier)
                    .take_if(|v| (min..=max).contains(v))
                {
                    Some(v) => Ok(Some(v)),
                    None => Err(format!("Decimal overflow, value `{n}`")),
                }
            }
            JsonbNumber::UInt64(n) => {
                match T::from_i128(n)
                    .and_then(|v| v.checked_mul(multiplier))
                    .take_if(|v| (min..=max).contains(v))
                {
                    Some(v) => Ok(Some(v)),
                    None => Err(format!("Decimal overflow, value `{n}`")),
                }
            }
            JsonbNumber::Float64(v) => {
                float_to_decimal(F64::from(v), min, max, multiplier_f64, rounding_mode)
                    .map(|v| Some(v))
            }
            JsonbNumber::Decimal64(d) => {
                let from_size = DecimalSize::new_unchecked(i64::MAX_PRECISION, d.scale);
                match dest_type {
                    DecimalDataType::Decimal64(_) => {
                        let x = d.value;
                        let min = i64::min_for_precision(dest_size.precision());
                        let max = i64::max_for_precision(dest_size.precision());
                        decimal_to_decimal(x, min, max, from_size, dest_size, rounding_mode)
                            .map(|v| Some(T::from_i64(v)))
                    }
                    DecimalDataType::Decimal128(_) | DecimalDataType::Decimal256(_) => {
                        let x = T::from_i64(d.value);
                        decimal_to_decimal(x, min, max, from_size, dest_size, rounding_mode)
                            .map(|v| Some(v))
                    }
                }
            }
            JsonbNumber::Decimal128(d) => {
                let from_size = DecimalSize::new_unchecked(i128::MAX_PRECISION, d.scale);
                match dest_type {
                    DecimalDataType::Decimal64(_) => {
                        let x = d.value;
                        let min = i128::min_for_precision(dest_size.precision());
                        let max = i128::max_for_precision(dest_size.precision());
                        decimal_to_decimal(x, min, max, from_size, dest_size, rounding_mode)
                            .map(|v| Some(T::from_i128_uncheck(v)))
                    }
                    DecimalDataType::Decimal128(_) | DecimalDataType::Decimal256(_) => {
                        let x = T::from_i128_uncheck(d.value);
                        decimal_to_decimal(x, min, max, from_size, dest_size, rounding_mode)
                            .map(|v| Some(v))
                    }
                }
            }
            JsonbNumber::Decimal256(d) => {
                let from_size = DecimalSize::new_unchecked(i256::MAX_PRECISION, d.scale);
                match dest_type {
                    DecimalDataType::Decimal64(_) | DecimalDataType::Decimal128(_) => {
                        let x = i256(d.value);
                        let min = i256::min_for_precision(dest_size.precision());
                        let max = i256::max_for_precision(dest_size.precision());
                        decimal_to_decimal(x, min, max, from_size, dest_size, rounding_mode)
                            .map(|v| Some(T::from_i256_uncheck(v)))
                    }
                    DecimalDataType::Decimal256(_) => {
                        let x = T::from_i256_uncheck(i256(d.value));
                        decimal_to_decimal(x, min, max, from_size, dest_size, rounding_mode)
                            .map(|v| Some(v))
                    }
                }
            }
        },
        _ => {
            if only_cast_number {
                return Ok(None);
            }
            match value {
                JsonbValue::Null => Ok(None),
                JsonbValue::Bool(b) => {
                    let v = if b {
                        T::e(dest_size.scale())
                    } else {
                        T::zero()
                    };
                    Ok(Some(v))
                }
                JsonbValue::String(s) => {
                    read_decimal_with_size::<T>(s.as_bytes(), dest_size, true, rounding_mode)
                        .map_err(|e| {
                            format!("Failed to cast to variant value `{s}` to decimal, {e}")
                        })
                        .map(|(v, _)| Some(v))
                }
                _ => Err("Unsupported json type".to_string()),
            }
        }
    }
}

fn float_to_decimal<T, N>(
    x: N,
    min: T,
    max: T,
    multiplier: f64,
    rounding_mode: bool,
) -> Result<T, String>
where
    T: Decimal,
    N: Number + AsPrimitive<f64> + std::fmt::Display,
{
    let mut value = x.as_() * multiplier;
    if rounding_mode {
        value = value.round();
    }
    let y = T::from_float(value);
    if y <= max && y >= min {
        return Ok(y);
    }
    Err(format!("Decimal overflow, value `{x}`"))
}

fn decimal_to_decimal<T: Decimal>(
    x: T,
    min: T,
    max: T,
    from_size: DecimalSize,
    dest_size: DecimalSize,
    rounding_mode: bool,
) -> Result<T, String> {
    if from_size.scale() == dest_size.scale() && from_size.precision() <= dest_size.precision() {
        Ok(x)
    } else if from_size.scale() > dest_size.scale() {
        let scale_diff = from_size.scale() - dest_size.scale();
        match decimal_scale_reduction(
            x,
            min,
            max,
            T::e(scale_diff),
            from_size.scale(),
            scale_diff,
            rounding_mode,
        ) {
            Some(y) => Ok(y),
            _ => Err("Decimal overflow".to_string()),
        }
    } else {
        let factor = T::e(dest_size.scale() - from_size.scale());
        match x.checked_mul(factor) {
            Some(y) if y <= max && y >= min => Ok(y),
            _ => Err("Decimal overflow".to_string()),
        }
    }
}
