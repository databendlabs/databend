// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datavalues::{BooleanArray, DataArrayRef, DataColumnarValue, DataValueLogicOperator};
use crate::error::{FuseQueryError, FuseQueryResult};

pub fn data_array_logic_op(
    op: DataValueLogicOperator,
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => match op {
            DataValueLogicOperator::And => {
                array_boolean_op!(left_array, right_array, and, BooleanArray)
            }
            DataValueLogicOperator::Or => {
                array_boolean_op!(left_array, right_array, or, BooleanArray)
            }
        },
        _ => Err(FuseQueryError::build_internal_error(format!(
            "Cannot do data_array {}, left:{:?}, right:{:?}",
            op,
            left.data_type(),
            right.data_type()
        ))),
    }
}
