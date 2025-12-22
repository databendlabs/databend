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

pub mod arithmetics_type;
pub mod arrow;
pub mod block_debug;
pub mod block_thresholds;
mod column_from;
pub mod date_helper;
pub mod display;
pub mod filter_helper;
pub mod serialize;
pub mod udf_client;
pub mod variant_transform;
pub mod visitor;

use databend_common_ast::Span;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub use self::column_from::*;
use crate::BlockEntry;
use crate::Column;
use crate::DataBlock;
use crate::Evaluator;
use crate::FunctionContext;
use crate::FunctionRegistry;
use crate::RawExpr;
use crate::Scalar;
use crate::Value;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::Decimal;
use crate::types::DecimalDataKind;
use crate::types::DecimalDataType;
use crate::types::DecimalSize;
use crate::types::F32;
use crate::types::F64;
use crate::types::NumberDataType;
use crate::types::NumberScalar;
use crate::types::decimal::DecimalScalar;
use crate::types::i256;

/// A convenient shortcut to evaluate a scalar function.
pub fn eval_function(
    span: Span,
    fn_name: &str,
    args: impl IntoIterator<Item = (Value<AnyType>, DataType)>,
    func_ctx: &FunctionContext,
    num_rows: usize,
    fn_registry: &FunctionRegistry,
) -> Result<(Value<AnyType>, DataType)> {
    let (args, cols) = args
        .into_iter()
        .enumerate()
        .map(|(id, (val, ty))| {
            (
                RawExpr::ColumnRef {
                    span,
                    id,
                    data_type: ty.clone(),
                    display_name: String::new(),
                },
                BlockEntry::new(val, || (ty, num_rows)),
            )
        })
        .unzip();
    let raw_expr = RawExpr::FunctionCall {
        span,
        name: fn_name.to_string(),
        params: vec![],
        args,
    };
    let expr = crate::type_check::check(&raw_expr, fn_registry)?;
    let block = DataBlock::new(cols, num_rows);
    let evaluator = Evaluator::new(&block, func_ctx, fn_registry);
    Ok((evaluator.run(&expr)?, expr.data_type().clone()))
}

pub fn cast_scalar(
    span: Span,
    scalar: Scalar,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Result<Scalar> {
    if let Some(result) = try_fast_cast_scalar(&scalar, dest_type) {
        return result;
    }

    let raw_expr = RawExpr::Cast {
        span,
        is_try: false,
        expr: Box::new(RawExpr::Constant {
            span,
            scalar,
            data_type: None,
        }),
        dest_type: dest_type.clone(),
    };
    let expr = crate::type_check::check(&raw_expr, fn_registry)?;
    let block = DataBlock::empty();
    let func_ctx = &FunctionContext::default();
    let evaluator = Evaluator::new(&block, func_ctx, fn_registry);
    Ok(evaluator.run(&expr)?.into_scalar().unwrap())
}

fn try_fast_cast_scalar(scalar: &Scalar, dest_type: &DataType) -> Option<Result<Scalar>> {
    match dest_type {
        DataType::Null => Some(Ok(Scalar::Null)),
        DataType::Nullable(inner) => {
            if matches!(scalar, Scalar::Null) {
                Some(Ok(Scalar::Null))
            } else {
                try_fast_cast_scalar(scalar, inner)
            }
        }
        DataType::Number(NumberDataType::Float32) => match scalar {
            Scalar::Null => Some(Ok(Scalar::Null)),
            Scalar::Number(num) => Some(Ok(Scalar::Number(NumberScalar::Float32(num.to_f32())))),
            Scalar::Decimal(dec) => Some(Ok(Scalar::Number(NumberScalar::Float32(F32::from(
                dec.to_float32(),
            ))))),
            _ => None,
        },
        DataType::Number(NumberDataType::Float64) => match scalar {
            Scalar::Null => Some(Ok(Scalar::Null)),
            Scalar::Number(num) => Some(Ok(Scalar::Number(NumberScalar::Float64(num.to_f64())))),
            Scalar::Decimal(dec) => Some(Ok(Scalar::Number(NumberScalar::Float64(F64::from(
                dec.to_float64(),
            ))))),
            _ => None,
        },
        DataType::Decimal(size) => match scalar {
            Scalar::Null => Some(Ok(Scalar::Null)),
            Scalar::Decimal(dec) => Some(rescale_decimal_scalar(*dec, *size)),
            _ => None,
        },
        _ => None,
    }
}

fn rescale_decimal_scalar(decimal: DecimalScalar, target_size: DecimalSize) -> Result<Scalar> {
    let from_size = decimal.size();
    if from_size == target_size {
        return Ok(Scalar::Decimal(decimal));
    }

    let source_scale = from_size.scale();
    let target_scale = target_size.scale();
    let data_type: DecimalDataType = target_size.into();

    let scaled = match data_type {
        DecimalDataType::Decimal64(_) => {
            let value = decimal.as_decimal::<i64>();
            let adjusted = rescale_decimal_value(value, source_scale, target_scale)?;
            Scalar::Decimal(DecimalScalar::Decimal64(adjusted, target_size))
        }
        DecimalDataType::Decimal128(_) => {
            let value = decimal.as_decimal::<i128>();
            let adjusted = rescale_decimal_value(value, source_scale, target_scale)?;
            Scalar::Decimal(DecimalScalar::Decimal128(adjusted, target_size))
        }
        DecimalDataType::Decimal256(_) => {
            let value = decimal.as_decimal::<i256>();
            let adjusted = rescale_decimal_value(value, source_scale, target_scale)?;
            Scalar::Decimal(DecimalScalar::Decimal256(adjusted, target_size))
        }
    };

    Ok(scaled)
}

fn rescale_decimal_value<T: Decimal>(value: T, source_scale: u8, target_scale: u8) -> Result<T> {
    if source_scale == target_scale {
        return Ok(value);
    }

    let diff = target_scale.abs_diff(source_scale);
    if target_scale > source_scale {
        value.checked_mul(T::e(diff)).ok_or_else(|| {
            ErrorCode::Overflow("Decimal literal overflow after scale expansion".to_string())
        })
    } else {
        value.checked_div(T::e(diff)).ok_or_else(|| {
            ErrorCode::Overflow("Decimal literal overflow after scale reduction".to_string())
        })
    }
}

pub fn column_merge_validity(entry: &BlockEntry, bitmap: Option<Bitmap>) -> Option<Bitmap> {
    match entry {
        BlockEntry::Const(scalar, data_type, n) => {
            if scalar.is_null() && data_type.is_nullable() {
                Some(Bitmap::new_zeroed(*n))
            } else {
                bitmap
            }
        }
        BlockEntry::Column(Column::Nullable(c)) => match bitmap {
            None => {
                let validity = c.validity();
                if validity.null_count() == 0 {
                    None
                } else {
                    Some(validity.clone())
                }
            }
            Some(v) => Some(&c.validity & (&v)),
        },
        _ => bitmap,
    }
}

pub fn shrink_scalar(scalar: Scalar) -> Scalar {
    match scalar {
        Scalar::Number(NumberScalar::UInt8(n)) => shrink_u64(n as u64),
        Scalar::Number(NumberScalar::UInt16(n)) => shrink_u64(n as u64),
        Scalar::Number(NumberScalar::UInt32(n)) => shrink_u64(n as u64),
        Scalar::Number(NumberScalar::UInt64(n)) => shrink_u64(n),
        Scalar::Number(NumberScalar::Int8(n)) => shrink_i64(n as i64),
        Scalar::Number(NumberScalar::Int16(n)) => shrink_i64(n as i64),
        Scalar::Number(NumberScalar::Int32(n)) => shrink_i64(n as i64),
        Scalar::Number(NumberScalar::Int64(n)) => shrink_i64(n),
        Scalar::Decimal(DecimalScalar::Decimal64(d, size)) => shrink_d256(d.into(), size),
        Scalar::Decimal(DecimalScalar::Decimal128(d, size)) => shrink_d256(d.into(), size),
        Scalar::Decimal(DecimalScalar::Decimal256(d, size)) => shrink_d256(d, size),
        Scalar::Tuple(mut fields) => {
            for field in fields.iter_mut() {
                *field = shrink_scalar(field.clone());
            }
            Scalar::Tuple(fields)
        }
        _ => scalar,
    }
}

fn shrink_u64(num: u64) -> Scalar {
    if num <= u8::MAX as u64 {
        Scalar::Number(NumberScalar::UInt8(num as u8))
    } else if num <= u16::MAX as u64 {
        Scalar::Number(NumberScalar::UInt16(num as u16))
    } else if num <= u32::MAX as u64 {
        Scalar::Number(NumberScalar::UInt32(num as u32))
    } else {
        Scalar::Number(NumberScalar::UInt64(num))
    }
}

fn shrink_i64(num: i64) -> Scalar {
    if num >= 0 {
        return shrink_u64(num as u64);
    }

    if num <= i8::MAX as i64 && num >= i8::MIN as i64 {
        Scalar::Number(NumberScalar::Int8(num as i8))
    } else if num <= i16::MAX as i64 && num >= i16::MIN as i64 {
        Scalar::Number(NumberScalar::Int16(num as i16))
    } else if num <= i32::MAX as i64 && num >= i32::MIN as i64 {
        Scalar::Number(NumberScalar::Int32(num as i32))
    } else {
        Scalar::Number(NumberScalar::Int64(num))
    }
}

fn shrink_d256(decimal: i256, size: DecimalSize) -> Scalar {
    if size.scale() == 0 {
        if decimal.is_positive() && decimal <= i256::from(u64::MAX) {
            return shrink_u64(decimal.as_u64());
        } else if decimal <= i256::from(i64::MAX) && decimal >= i256::from(i64::MIN) {
            return shrink_i64(decimal.as_i64());
        }
    }

    let valid_bits = 256 - decimal.saturating_abs().leading_zeros();
    let log10_2 = std::f64::consts::LOG10_2;
    let mut precision = ((valid_bits as f64) * log10_2).floor() as u8;

    if decimal.saturating_abs() >= i256::e(precision) {
        precision += 1;
    }

    // adjust precision to the maximum scale of the decimal type
    if precision < size.scale() {
        precision = size.scale();
    }
    precision = precision.clamp(1, i256::MAX_PRECISION);

    let size = DecimalSize::new(precision, size.scale()).unwrap();
    match size.data_kind() {
        DecimalDataKind::Decimal64 => {
            Scalar::Decimal(DecimalScalar::Decimal64(decimal.as_i64(), size))
        }
        DecimalDataKind::Decimal128 => {
            Scalar::Decimal(DecimalScalar::Decimal128(decimal.as_i128(), size))
        }
        DecimalDataKind::Decimal256 => Scalar::Decimal(DecimalScalar::Decimal256(decimal, size)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shrink_scalar() {
        let tests = [
            (
                Scalar::Decimal(DecimalScalar::Decimal64(
                    50,
                    DecimalSize::new_unchecked(10, 1),
                )),
                Scalar::Decimal(DecimalScalar::Decimal64(
                    50,
                    DecimalSize::new_unchecked(2, 1),
                )),
            ),
            (
                Scalar::Decimal(DecimalScalar::Decimal64(
                    50,
                    DecimalSize::new_unchecked(10, 0),
                )),
                Scalar::Number(NumberScalar::UInt8(50)),
            ),
        ];

        for (t, want) in tests {
            assert_eq!(shrink_scalar(t), want);
        }
    }
}
