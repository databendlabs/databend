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
use databend_common_exception::Result;

pub use self::column_from::*;
use crate::types::decimal::DecimalScalar;
use crate::types::i256;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::Decimal;
use crate::types::DecimalDataKind;
use crate::types::DecimalSize;
use crate::types::NumberScalar;
use crate::BlockEntry;
use crate::Column;
use crate::DataBlock;
use crate::Evaluator;
use crate::FunctionContext;
use crate::FunctionRegistry;
use crate::RawExpr;
use crate::Scalar;
use crate::Value;

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
    dest_type: DataType,
    fn_registry: &FunctionRegistry,
) -> Result<Scalar> {
    let raw_expr = RawExpr::Cast {
        span,
        is_try: false,
        expr: Box::new(RawExpr::Constant {
            span,
            scalar,
            data_type: None,
        }),
        dest_type,
    };
    let expr = crate::type_check::check(&raw_expr, fn_registry)?;
    let block = DataBlock::empty();
    let func_ctx = &FunctionContext::default();
    let evaluator = Evaluator::new(&block, func_ctx, fn_registry);
    Ok(evaluator.run(&expr)?.into_scalar().unwrap())
}

pub fn column_merge_validity(column: &Column, bitmap: Option<Bitmap>) -> Option<Bitmap> {
    match column {
        Column::Nullable(c) => match bitmap {
            None => Some(c.validity.clone()),
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
