// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::{
    data_array_arithmetic_op, DataColumnarValue, DataValue, DataValueArithmeticOperator,
};
use crate::error::FuseQueryResult;

pub fn data_value_arithmetic_op(
    op: DataValueArithmeticOperator,
    left: DataValue,
    right: DataValue,
) -> FuseQueryResult<DataValue> {
    match (&left, &right) {
        (DataValue::Null, _) => Ok(right),
        (_, DataValue::Null) => Ok(left),
        _ => {
            let result = data_array_arithmetic_op(
                op,
                &DataColumnarValue::Scalar(left),
                &DataColumnarValue::Scalar(right),
            )?;
            DataValue::try_from_array(&result, 0)
        }
    }
}
