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

use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use ordered_float::OrderedFloat;

use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::ScalarExpr;

pub fn is_true(predicate: &ScalarExpr) -> bool {
    matches!(
        predicate,
        ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::Boolean(true),
            ..
        })
    )
}

pub fn is_falsy(predicate: &ScalarExpr) -> bool {
    matches!(
        predicate,
        ScalarExpr::ConstantExpr(ConstantExpr {
            value,
            ..
        }) if value == &Scalar::Boolean(false) || value == &Scalar::Null
    )
}

pub fn false_constant() -> ScalarExpr {
    ConstantExpr {
        span: None,
        value: Scalar::Boolean(false),
    }
    .into()
}

pub fn check_uint_range(max: u64, value: &Scalar) -> (bool, u64) {
    let value = match *value {
        Scalar::Number(NumberScalar::UInt8(value)) => value as u64,
        Scalar::Number(NumberScalar::UInt16(value)) => value as u64,
        Scalar::Number(NumberScalar::UInt32(value)) => value as u64,
        Scalar::Number(NumberScalar::UInt64(value)) => value,
        Scalar::Number(NumberScalar::Int8(value)) if value >= 0 => value as u64,
        Scalar::Number(NumberScalar::Int16(value)) if value >= 0 => value as u64,
        Scalar::Number(NumberScalar::Int32(value)) if value >= 0 => value as u64,
        Scalar::Number(NumberScalar::Int64(value)) if value >= 0 => value as u64,
        _ => return (false, 0),
    };
    (value <= max, value)
}

pub fn check_int_range(min: i64, max: i64, value: &Scalar) -> (bool, i64) {
    let value = match *value {
        Scalar::Number(NumberScalar::UInt8(value)) => value as i64,
        Scalar::Number(NumberScalar::UInt16(value)) => value as i64,
        Scalar::Number(NumberScalar::UInt32(value)) => value as i64,
        Scalar::Number(NumberScalar::UInt64(value)) if value <= i64::MAX as u64 => value as i64,
        Scalar::Number(NumberScalar::Int8(value)) => value as i64,
        Scalar::Number(NumberScalar::Int16(value)) => value as i64,
        Scalar::Number(NumberScalar::Int32(value)) => value as i64,
        Scalar::Number(NumberScalar::Int64(value)) => value,
        _ => return (false, 0),
    };
    (value >= min && value <= max, value)
}

pub fn check_float_range(min: f64, max: f64, value: &Scalar) -> (bool, f64) {
    let value = match *value {
        Scalar::Number(NumberScalar::Float32(value)) => value.into_inner() as f64,
        Scalar::Number(NumberScalar::Float64(value)) => value.into_inner(),
        _ => return (false, 0.0),
    };
    (value >= min && value <= max, value)
}

pub fn remove_trivial_type_cast(left: ScalarExpr, right: ScalarExpr) -> (ScalarExpr, ScalarExpr) {
    match (&left, &right) {
        (
            ScalarExpr::CastExpr(CastExpr { argument, .. }),
            ScalarExpr::ConstantExpr(ConstantExpr { span, value }),
        )
        | (
            ScalarExpr::ConstantExpr(ConstantExpr { span, value }),
            ScalarExpr::CastExpr(CastExpr { argument, .. }),
        ) => {
            if let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = &(**argument) {
                match *column.data_type {
                    DataType::Number(NumberDataType::UInt8)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt8)) => {
                        let (is_trivial, v) = check_uint_range(u8::MAX as u64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt8(v as u8)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::UInt16)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt16)) => {
                        let (is_trivial, v) = check_uint_range(u16::MAX as u64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt16(v as u16)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::UInt32)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt32)) => {
                        let (is_trivial, v) = check_uint_range(u32::MAX as u64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt32(v as u32)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::UInt64)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt64)) => {
                        let (is_trivial, v) = check_uint_range(u64::MAX, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt64(v)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int8)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int8)) => {
                        let (is_trivial, v) =
                            check_int_range(i8::MIN as i64, i8::MAX as i64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int8(v as i8)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int16)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int16)) => {
                        let (is_trivial, v) =
                            check_int_range(i16::MIN as i64, i16::MAX as i64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int16(v as i16)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int32)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int32)) => {
                        let (is_trivial, v) =
                            check_int_range(i32::MIN as i64, i32::MAX as i64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int32(v as i32)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int64)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int64)) => {
                        let (is_trivial, v) = check_int_range(i64::MIN, i64::MAX, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(v.into()),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Float32)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Float32)) => {
                        let (is_trivial, v) =
                            check_float_range(f32::MIN as f64, f32::MAX as f64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Float32(OrderedFloat(
                                        v as f32,
                                    ))),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Float64)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Float64)) => {
                        let (is_trivial, v) = check_float_range(f64::MIN, f64::MAX, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Float64(OrderedFloat(v))),
                                }),
                            );
                        }
                    }
                    _ => (),
                };
                (left, right)
            } else {
                (left, right)
            }
        }
        _ => (left, right),
    }
}
