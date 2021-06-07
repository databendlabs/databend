// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;

use crate::data_array_cast;
use crate::DataArrayRef;
use crate::DataColumnarValue;
use crate::DataType;
use crate::DataValue;
use crate::DataValueComparisonOperator;
use crate::Float32Array;
use crate::Float64Array;
use crate::Int16Array;
use crate::Int32Array;
use crate::Int64Array;
use crate::Int8Array;
use crate::StringArray;
use crate::UInt16Array;
use crate::UInt32Array;
use crate::UInt64Array;
use crate::UInt8Array;

pub struct DataArrayComparison;

impl DataArrayComparison {
    #[inline]
    pub fn data_array_comparison_op(
        op: DataValueComparisonOperator,
        left: &DataColumnarValue,
        right: &DataColumnarValue,
    ) -> Result<DataArrayRef> {
        match (left, right) {
            (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => {
                let coercion_type = super::data_type::equal_coercion(
                    &left_array.data_type(),
                    &right_array.data_type(),
                )?;
                let left_array = data_array_cast(&left_array, &coercion_type)?;
                let right_array = data_array_cast(&right_array, &coercion_type)?;

                match op {
                    DataValueComparisonOperator::Eq => {
                        arrow_array_op!(&left_array, &right_array, eq)
                    }
                    DataValueComparisonOperator::Lt => {
                        arrow_array_op!(&left_array, &right_array, lt)
                    }
                    DataValueComparisonOperator::LtEq => {
                        arrow_array_op!(&left_array, &right_array, lt_eq)
                    }
                    DataValueComparisonOperator::Gt => {
                        arrow_array_op!(&left_array, &right_array, gt)
                    }
                    DataValueComparisonOperator::GtEq => {
                        arrow_array_op!(&left_array, &right_array, gt_eq)
                    }
                    DataValueComparisonOperator::NotEq => {
                        arrow_array_op!(&left_array, &right_array, neq)
                    }
                }
            }

            (DataColumnarValue::Array(array), DataColumnarValue::Constant(scalar, _)) => {
                let coercion_type =
                    super::data_type::equal_coercion(&array.data_type(), &scalar.data_type())?;
                let left_array = data_array_cast(&array, &coercion_type)?;
                let right_array = data_array_cast(&scalar.to_array_with_size(1)?, &coercion_type)?;
                let scalar = DataValue::try_from_array(&right_array, 0)?;

                match op {
                    DataValueComparisonOperator::Eq => {
                        arrow_array_op_scalar!(left_array, scalar, eq)
                    }
                    DataValueComparisonOperator::Lt => {
                        arrow_array_op_scalar!(left_array, scalar, lt)
                    }
                    DataValueComparisonOperator::LtEq => {
                        arrow_array_op_scalar!(left_array, scalar, lt_eq)
                    }
                    DataValueComparisonOperator::Gt => {
                        arrow_array_op_scalar!(left_array, scalar, gt)
                    }
                    DataValueComparisonOperator::GtEq => {
                        arrow_array_op_scalar!(left_array, scalar, gt_eq)
                    }
                    DataValueComparisonOperator::NotEq => {
                        arrow_array_op_scalar!(left_array, scalar, neq)
                    }
                }
            }

            (DataColumnarValue::Constant(scalar, _), DataColumnarValue::Array(array)) => {
                let coercion_type =
                    super::data_type::equal_coercion(&array.data_type(), &scalar.data_type())?;
                let left_array = data_array_cast(&scalar.to_array_with_size(1)?, &coercion_type)?;
                let right_array = data_array_cast(&array, &coercion_type)?;
                let scalar = DataValue::try_from_array(&left_array, 0)?;

                match op {
                    DataValueComparisonOperator::Eq => {
                        arrow_array_op_scalar!(right_array, scalar, eq)
                    }
                    DataValueComparisonOperator::Lt => {
                        arrow_array_op_scalar!(right_array, scalar, gt)
                    }
                    DataValueComparisonOperator::LtEq => {
                        arrow_array_op_scalar!(right_array, scalar, gt_eq)
                    }
                    DataValueComparisonOperator::Gt => {
                        arrow_array_op_scalar!(right_array, scalar, lt)
                    }
                    DataValueComparisonOperator::GtEq => {
                        arrow_array_op_scalar!(right_array, scalar, lt_eq)
                    }
                    DataValueComparisonOperator::NotEq => {
                        arrow_array_op_scalar!(right_array, scalar, neq)
                    }
                }
            }
            (
                DataColumnarValue::Constant(left_scala, rows),
                DataColumnarValue::Constant(right_scalar, _),
            ) => {
                let coercion_type = super::data_type::equal_coercion(
                    &left_scala.data_type(),
                    &right_scalar.data_type(),
                )?;
                let left_array =
                    data_array_cast(&left_scala.to_array_with_size(*rows)?, &coercion_type)?;
                let right_array =
                    data_array_cast(&right_scalar.to_array_with_size(*rows)?, &coercion_type)?;

                match op {
                    DataValueComparisonOperator::Eq => {
                        arrow_array_op!(&left_array, &right_array, eq)
                    }
                    DataValueComparisonOperator::Lt => {
                        arrow_array_op!(&left_array, &right_array, lt)
                    }
                    DataValueComparisonOperator::LtEq => {
                        arrow_array_op!(&left_array, &right_array, lt_eq)
                    }
                    DataValueComparisonOperator::Gt => {
                        arrow_array_op!(&left_array, &right_array, gt)
                    }
                    DataValueComparisonOperator::GtEq => {
                        arrow_array_op!(&left_array, &right_array, gt_eq)
                    }
                    DataValueComparisonOperator::NotEq => {
                        arrow_array_op!(&left_array, &right_array, neq)
                    }
                }
            }
        }
    }
}
