// Copyright 2021 Datafuse Labs.
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

use std::cmp;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::TypeID::*;
use crate::prelude::*;
use crate::types::data_type::DataTypePtr;
use crate::DataValueBinaryOperator;
use crate::DataValueUnaryOperator;

fn next_size(size: usize) -> usize {
    if size < 8_usize {
        return size * 2;
    }
    size
}

pub fn construct_numeric_type(
    is_signed: bool,
    is_floating: bool,
    byte_size: usize,
) -> Result<DataTypePtr> {
    match (is_signed, is_floating, byte_size) {
        (false, false, 1) => Ok(UInt8Type::arc()),
        (false, false, 2) => Ok(UInt16Type::arc()),
        (false, false, 4) => Ok(UInt32Type::arc()),
        (false, false, 8) => Ok(UInt64Type::arc()),
        (false, true, 4) => Ok(Float32Type::arc()),
        (false, true, 8) => Ok(Float64Type::arc()),
        (true, false, 1) => Ok(Int8Type::arc()),
        (true, false, 2) => Ok(Int16Type::arc()),
        (true, false, 4) => Ok(Int32Type::arc()),
        (true, false, 8) => Ok(Int64Type::arc()),
        (true, true, 1) => Ok(Float32Type::arc()),
        (true, true, 2) => Ok(Float32Type::arc()),
        (true, true, 4) => Ok(Float32Type::arc()),
        (true, true, 8) => Ok(Float64Type::arc()),

        // TODO support bigint and decimal types, now we just let's overflow
        (false, false, d) if d > 8 => Ok(Int64Type::arc()),
        (true, false, d) if d > 8 => Ok(UInt64Type::arc()),
        (_, true, d) if d > 8 => Ok(Float64Type::arc()),

        _ => Result::Err(ErrorCode::BadDataValueType(format!(
            "Can't construct type from is_signed: {}, is_floating: {}, byte_size: {}",
            is_signed, is_floating, byte_size
        ))),
    }
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
pub fn numerical_coercion(
    lhs_type: &DataTypePtr,
    rhs_type: &DataTypePtr,
    allow_overflow: bool,
) -> Result<DataTypePtr> {
    let lhs_id = lhs_type.data_type_id();
    let rhs_id = rhs_type.data_type_id();

    let has_float = lhs_id.is_floating() || rhs_id.is_floating();
    let has_integer = lhs_id.is_integer() || rhs_id.is_integer();
    let has_signed = lhs_id.is_signed_numeric() || rhs_id.is_signed_numeric();

    let size_of_lhs = lhs_id.numeric_byte_size()?;
    let size_of_rhs = rhs_id.numeric_byte_size()?;

    let max_size_of_unsigned_integer = cmp::max(
        if lhs_id.is_signed_numeric() {
            0
        } else {
            size_of_lhs
        },
        if rhs_id.is_signed_numeric() {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_signed_integer = cmp::max(
        if !lhs_id.is_signed_numeric() {
            0
        } else {
            size_of_lhs
        },
        if !rhs_id.is_signed_numeric() {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_integer = cmp::max(
        if !lhs_id.is_integer() { 0 } else { size_of_lhs },
        if !rhs_id.is_integer() { 0 } else { size_of_rhs },
    );

    let max_size_of_float = cmp::max(
        if !lhs_id.is_floating() {
            0
        } else {
            size_of_lhs
        },
        if !rhs_id.is_floating() {
            0
        } else {
            size_of_rhs
        },
    );

    let should_double = (has_float && has_integer && max_size_of_integer >= max_size_of_float)
        || (has_signed && max_size_of_unsigned_integer >= max_size_of_signed_integer);

    let mut max_size = if should_double {
        cmp::max(size_of_rhs, size_of_lhs) * 2
    } else {
        cmp::max(size_of_rhs, size_of_lhs)
    };

    if max_size > 8 {
        if allow_overflow {
            max_size = 8
        } else {
            return Result::Err(ErrorCode::BadDataValueType(format!(
                "Can't construct type from {:?} and {:?}",
                lhs_type, rhs_type
            )));
        }
    }

    construct_numeric_type(has_signed, has_float, max_size)
}

#[inline]
pub fn numerical_arithmetic_coercion(
    op: &DataValueBinaryOperator,
    lhs_type: &DataTypePtr,
    rhs_type: &DataTypePtr,
) -> Result<DataTypePtr> {
    let lhs_id = lhs_type.data_type_id();
    let rhs_id = rhs_type.data_type_id();

    // error on any non-numeric type
    if !lhs_id.is_numeric() || !rhs_id.is_numeric() {
        return Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        )));
    };

    let has_signed = lhs_id.is_signed_numeric() || rhs_id.is_signed_numeric();
    let has_float = lhs_id.is_floating() || rhs_id.is_floating();
    let max_size = cmp::max(lhs_id.numeric_byte_size()?, rhs_id.numeric_byte_size()?);

    match op {
        DataValueBinaryOperator::Plus | DataValueBinaryOperator::Mul => {
            construct_numeric_type(has_signed, has_float, next_size(max_size))
        }

        DataValueBinaryOperator::Modulo => {
            if has_float {
                return Ok(Float64Type::arc());
            }
            // From clickhouse: NumberTraits.h
            // If modulo of division can yield negative number, we need larger type to accommodate it.
            // Example: toInt32(-199) % toUInt8(200) will return -199 that does not fit in Int8, only in Int16.
            let result_is_signed = lhs_id.is_signed_numeric();
            let right_size = rhs_id.numeric_byte_size()?;
            let size_of_result = if result_is_signed {
                next_size(right_size)
            } else {
                right_size
            };
            construct_numeric_type(result_is_signed, false, size_of_result)
        }
        DataValueBinaryOperator::Minus => {
            construct_numeric_type(true, has_float, next_size(max_size))
        }
        DataValueBinaryOperator::Div => Ok(Float64Type::arc()),
        DataValueBinaryOperator::IntDiv => construct_numeric_type(has_signed, false, max_size),
    }
}

#[inline]
pub fn datetime_arithmetic_coercion(
    op: &DataValueBinaryOperator,
    lhs_type: &DataTypePtr,
    rhs_type: &DataTypePtr,
) -> Result<DataTypePtr> {
    let e = || {
        Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        )))
    };

    let lhs_id = lhs_type.data_type_id();
    let rhs_id = rhs_type.data_type_id();

    if !lhs_id.is_date_or_date_time() && !rhs_id.is_date_or_date_time() {
        return e();
    }

    let mut a = lhs_type.clone();
    let mut b = rhs_type.clone();
    if !a.data_type_id().is_date_or_date_time() {
        a = rhs_type.clone();
        b = lhs_type.clone();
    }

    match op {
        DataValueBinaryOperator::Plus => Ok(a),

        DataValueBinaryOperator::Minus => {
            if b.data_type_id().is_numeric() || b.data_type_id().is_interval() {
                Ok(a)
            } else {
                // Date minus Date or DateTime minus DateTime
                Ok(Int32Type::arc())
            }
        }
        _ => e(),
    }
}

#[inline]
pub fn interval_arithmetic_coercion(
    op: &DataValueBinaryOperator,
    lhs_type: &DataTypePtr,
    rhs_type: &DataTypePtr,
) -> Result<DataTypePtr> {
    let e = || {
        Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        )))
    };

    let lhs_id = lhs_type.data_type_id();
    let rhs_id = rhs_type.data_type_id();

    // only allow date/datetime [+/-] interval
    if !(lhs_id.is_date_or_date_time() && rhs_id.is_interval()
        || rhs_id.is_date_or_date_time() && lhs_id.is_interval())
    {
        return e();
    }

    match op {
        DataValueBinaryOperator::Plus | DataValueBinaryOperator::Minus => {
            if lhs_id.is_date_or_date_time() {
                Ok(lhs_type.clone())
            } else {
                Ok(rhs_type.clone())
            }
        }
        _ => e(),
    }
}

#[inline]
pub fn numerical_unary_arithmetic_coercion(
    op: &DataValueUnaryOperator,
    val_type: &DataTypePtr,
) -> Result<DataTypePtr> {
    let type_id = val_type.data_type_id();
    // error on any non-numeric type
    if !type_id.is_numeric() {
        return Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported ({:?})",
            type_id
        )));
    };

    match op {
        DataValueUnaryOperator::Negate => {
            let has_float = type_id.is_floating();
            let has_signed = type_id.is_signed_numeric();
            let numeric_size = type_id.numeric_byte_size()?;
            let max_size = if has_signed {
                numeric_size
            } else {
                next_size(numeric_size)
            };
            construct_numeric_type(true, has_float, max_size)
        }
    }
}

// coercion rules for compare operations. This is a superset of all numerical coercion rules.
pub fn compare_coercion(lhs_type: &DataTypePtr, rhs_type: &DataTypePtr) -> Result<DataTypePtr> {
    let lhs_id = lhs_type.data_type_id();
    let rhs_id = rhs_type.data_type_id();

    if lhs_id == rhs_id {
        // same type => equality is possible
        return Ok(lhs_type.clone());
    }

    if lhs_id.is_numeric() && rhs_id.is_numeric() {
        return numerical_coercion(lhs_type, rhs_type, true);
    }

    //  one of is nothing
    {
        if lhs_id == TypeID::Null {
            return Ok(wrap_nullable(rhs_type));
        }

        if rhs_id == TypeID::Null {
            return Ok(wrap_nullable(lhs_type));
        }
    }

    // one of is String and other is number
    if (lhs_id.is_numeric() && rhs_id.is_string()) || (rhs_id.is_numeric() && rhs_id.is_string()) {
        return Ok(Float64Type::arc());
    }

    // one of is datetime and other is number or string
    {
        if (lhs_id.is_numeric() || lhs_id.is_string()) && rhs_id.is_date_or_date_time() {
            return Ok(rhs_type.clone());
        }

        if (rhs_id.is_numeric() || rhs_id.is_string()) && lhs_id.is_date_or_date_time() {
            return Ok(lhs_type.clone());
        }
    }

    // one of is datetime and other is number or string
    if lhs_id.is_date_or_date_time() || rhs_id.is_date_or_date_time() {
        // one of is datetime
        // TODO datetime64
        if matches!(lhs_id, TypeID::DateTime32) || matches!(rhs_id, TypeID::DateTime32) {
            return Ok(DateTime32Type::arc(None));
        }
        return Ok(Date32Type::arc());
    }

    Err(ErrorCode::IllegalDataType(format!(
        "Can not compare {:?} with {:?}",
        lhs_type, rhs_type
    )))
}

// aggregate_types aggregates data types for a multi-argument function.
#[inline]
pub fn aggregate_types(args: &[DataTypePtr]) -> Result<DataTypePtr> {
    match args.len() {
        0 => Result::Err(ErrorCode::BadArguments("Can't aggregate empty args")),
        1 => Ok(args[0].clone()),
        _ => {
            let left = args[0].clone();
            let right = aggregate_types(&args[1..args.len()])?;
            merge_types(&left, &right)
        }
    }
}

pub fn merge_types(lhs_type: &DataTypePtr, rhs_type: &DataTypePtr) -> Result<DataTypePtr> {
    if lhs_type.is_nullable() || rhs_type.is_nullable() {
        let lhs_type = remove_nullable(lhs_type);
        let rhs_type = remove_nullable(rhs_type);
        let merge_types = merge_types(&lhs_type, &rhs_type)?;
        return Ok(wrap_nullable(&merge_types));
    }

    let lhs_id = lhs_type.data_type_id();
    let rhs_id = rhs_type.data_type_id();

    match (lhs_id, rhs_id) {
        (Null, _) => Ok(wrap_nullable(rhs_type)),
        (_, Null) => Ok(wrap_nullable(lhs_type)),

        (Array, Array) => {
            let a = lhs_type.as_any().downcast_ref::<ArrayType>().unwrap();
            let b = rhs_type.as_any().downcast_ref::<ArrayType>().unwrap();

            let typ = merge_types(a.inner_type(), b.inner_type())?;
            Ok(Arc::new(ArrayType::create(typ)))
        }
        (Struct, Struct) => {
            let a = lhs_type.as_any().downcast_ref::<StructType>().unwrap();
            let b = rhs_type.as_any().downcast_ref::<StructType>().unwrap();
            if a.names() != b.names() {
                return Err(ErrorCode::BadArguments(
                    "Can't merge structs with different names or sizes".to_string(),
                ));
            }

            let types = a
                .types()
                .iter()
                .zip(b.types().iter())
                .map(|(a, b)| merge_types(a, b))
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(StructType::create(a.names().clone(), types)))
        }
        _ => {
            if lhs_id == rhs_id {
                return Ok(lhs_type.clone());
            }
            if lhs_id.is_numeric() && rhs_id.is_numeric() {
                numerical_coercion(lhs_type, rhs_type, false)
            } else {
                Result::Err(ErrorCode::BadDataValueType(format!(
                    "Can't merge types from {:?} and {:?}",
                    lhs_type, rhs_type
                )))
            }
        }
    }
}
