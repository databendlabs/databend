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

//! Typed cast entries shared by public casts and arithmetic input preparation.

use databend_common_expression::EvalContext;
use databend_common_expression::Value;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;

use super::decimal_cast;
use super::float_to_decimal;
use super::integer_to_decimal;
use crate::decimal::DecimalExt;

pub(crate) fn number<T: DecimalExt>(
    value: &Value<AnyType>,
    ty: NumberDataType,
    size: DecimalSize,
    ctx: &mut EvalContext,
) -> Value<DecimalType<T>> {
    match ty {
        NumberDataType::UInt8 => {
            integer_to_decimal::<T, NumberType<u8>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::UInt16 => {
            integer_to_decimal::<T, NumberType<u16>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::UInt32 => {
            integer_to_decimal::<T, NumberType<u32>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::UInt64 => {
            integer_to_decimal::<T, NumberType<u64>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::Int8 => {
            integer_to_decimal::<T, NumberType<i8>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::Int16 => {
            integer_to_decimal::<T, NumberType<i16>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::Int32 => {
            integer_to_decimal::<T, NumberType<i32>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::Int64 => {
            integer_to_decimal::<T, NumberType<i64>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::Float32 => {
            float_to_decimal::<T, NumberType<F32>>(value.try_downcast().unwrap(), ctx, size)
        }
        NumberDataType::Float64 => {
            float_to_decimal::<T, NumberType<F64>>(value.try_downcast().unwrap(), ctx, size)
        }
    }
}

// Each cast computes in the wider of its source and target storage.

pub(crate) fn decimal64(
    from: DecimalDataType,
    size: DecimalSize,
    value: &Value<AnyType>,
    ctx: &mut EvalContext,
) -> Value<Decimal64Type> {
    match from {
        DecimalDataType::Decimal64(from) => {
            decimal_cast::<i64, i64, i64>(from, size, value.try_downcast().unwrap(), ctx)
        }
        DecimalDataType::Decimal128(from) => {
            decimal_cast::<i128, i128, i64>(from, size, value.try_downcast().unwrap(), ctx)
        }
        DecimalDataType::Decimal256(from) => {
            decimal_cast::<i256, i256, i64>(from, size, value.try_downcast().unwrap(), ctx)
        }
    }
}

pub(crate) fn decimal128(
    from: DecimalDataType,
    size: DecimalSize,
    value: &Value<AnyType>,
    ctx: &mut EvalContext,
) -> Value<Decimal128Type> {
    match from {
        DecimalDataType::Decimal64(from) => {
            decimal_cast::<i64, i128, i128>(from, size, value.try_downcast().unwrap(), ctx)
        }
        DecimalDataType::Decimal128(from) => {
            decimal_cast::<i128, i128, i128>(from, size, value.try_downcast().unwrap(), ctx)
        }
        DecimalDataType::Decimal256(from) => {
            decimal_cast::<i256, i256, i128>(from, size, value.try_downcast().unwrap(), ctx)
        }
    }
}

pub(crate) fn decimal256(
    from: DecimalDataType,
    size: DecimalSize,
    value: &Value<AnyType>,
    ctx: &mut EvalContext,
) -> Value<Decimal256Type> {
    match from {
        DecimalDataType::Decimal64(from) => {
            decimal_cast::<i64, i256, i256>(from, size, value.try_downcast().unwrap(), ctx)
        }
        DecimalDataType::Decimal128(from) => {
            decimal_cast::<i128, i256, i256>(from, size, value.try_downcast().unwrap(), ctx)
        }
        DecimalDataType::Decimal256(from) => {
            decimal_cast::<i256, i256, i256>(from, size, value.try_downcast().unwrap(), ctx)
        }
    }
}
