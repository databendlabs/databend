// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;

use crate::BooleanArray;
use crate::DataArrayRef;
use crate::DataColumnarValue;
use crate::DataValueLogicOperator;

pub struct DataArrayLogic;

impl DataArrayLogic {
    #[inline]
    fn data_array_logic_binary(
        op: DataValueLogicOperator,
        left: &DataColumnarValue,
        right: &DataColumnarValue
    ) -> Result<DataArrayRef> {
        match (left, right) {
            (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => {
                if let DataValueLogicOperator::And = op {
                    array_boolean_op!(left_array, right_array, and, BooleanArray)
                } else {
                    array_boolean_op!(left_array, right_array, or, BooleanArray)
                }
            }
            _ => Result::Err(ErrorCodes::BadDataValueType(format!(
                "DataValue Error: Cannot do data_array {}, left:{:?}, right:{:?}",
                op,
                left.data_type(),
                right.data_type()
            )))
        }
    }

    fn data_array_negation(val: &DataColumnarValue) -> Result<DataArrayRef> {
        match val {
            DataColumnarValue::Array(array) => {
                let arr = downcast_array!(array, BooleanArray)?;
                Ok(Arc::new(
                    common_arrow::arrow::compute::not(arr).map_err(ErrorCodes::from)?
                ))
            }
            _ => Result::Err(ErrorCodes::BadDataValueType(format!(
                "DataValue Error: Cannot do data_array Not, array:{:?}",
                val.data_type(),
            )))
        }
    }

    pub fn data_array_logic_op(
        op: DataValueLogicOperator,
        args: &Vec<DataColumnarValue>
    ) -> Result<DataArrayRef> {
        match op {
            DataValueLogicOperator::Not => Self::data_array_negation(&args[0]),
            _ => Self::data_array_logic_binary(op, &args[0], &args[1])
        }
    }
}
