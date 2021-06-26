// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::BooleanArray;
use crate::Series;
use crate::DataColumnarValue;
use crate::DataValue;
use crate::DataValueLogicOperator;

pub struct DataArrayLogic;

impl DataArrayLogic {
    #[inline]
    fn data_array_logic_binary(
        op: DataValueLogicOperator,
        left: &DataColumnarValue,
        right: &DataColumnarValue,
    ) -> Result<Series> {
        match (left, right) {
            (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => {
                if let DataValueLogicOperator::And = op {
                    array_boolean_op!(left_array, right_array, and, BooleanArray)
                } else {
                    array_boolean_op!(left_array, right_array, or, BooleanArray)
                }
            }
            _ => Result::Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Cannot do data_array {}, left:{:?}, right:{:?}",
                op,
                left.data_type(),
                right.data_type()
            ))),
        }
    }

    #[allow(clippy::nonminimal_bool)]
    fn data_array_negation(val: &DataColumnarValue) -> Result<Series> {
        match val {
            DataColumnarValue::Array(array) => {
                let arr = downcast_array!(array, BooleanArray)?;
                Ok(Arc::new(
                    common_arrow::arrow::compute::not(arr).map_err(ErrorCode::from)?,
                ))
            }
            DataColumnarValue::Constant(v, size) => match v {
                DataValue::UInt64(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::UInt32(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::UInt16(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::UInt8(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::Int64(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::Int32(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::Int16(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::Int8(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi != 0)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                DataValue::Boolean(Some(vi)) => {
                    let vb = DataValue::Boolean(Some(!(*vi)));
                    Ok(DataColumnarValue::Constant(vb, *size).to_array()?)
                }
                _ => Result::Err(ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Cannot do negation for val:{:?}",
                    val
                ))),
            },
        }
    }

    pub fn data_array_logic_op(
        op: DataValueLogicOperator,
        args: &[DataColumnarValue],
    ) -> Result<Series> {
        match op {
            DataValueLogicOperator::Not => Self::data_array_negation(&args[0]),
            _ => Self::data_array_logic_binary(op, &args[0], &args[1]),
        }
    }
}
