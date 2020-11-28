// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::{BooleanArray, DataArrayRef, DataColumnarValue, DataValueLogicOperator};
use crate::error::{FuseQueryError, FuseQueryResult};

pub fn data_array_logic_op(
    op: DataValueLogicOperator,
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(larray), DataColumnarValue::Array(rarray)) => match op {
            DataValueLogicOperator::And => array_boolean_op!(larray, rarray, and, BooleanArray),
            DataValueLogicOperator::Or => array_boolean_op!(larray, rarray, or, BooleanArray),
        },
        _ => Err(FuseQueryError::Internal(format!(
            "Cannot do data_array {}, left:{:?}, right:{:?}",
            op,
            left.data_type(),
            right.data_type()
        ))),
    }
}
