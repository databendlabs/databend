// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::*;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::aggregator_common::assert_unary_arguments;
use crate::AggregateFunction;

#[derive(Clone)]
pub struct AggregateMaxFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateMaxFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;

        Ok(Box::new(AggregateMaxFunction {
            display_name: display_name.to_string(),
            state: DataValue::Null,
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateMaxFunction {
    fn name(&self) -> &str {
        "AggregateMaxFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn accumulate(&mut self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<()> {
        let value = Self::max_batch(columns[0].clone())?;
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Max,
            self.state.clone(),
            value,
        )?;

        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[0].clone();
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Max,
            self.state.clone(),
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateMaxFunction {
    pub fn max_batch(column: DataColumnarValue) -> Result<DataValue> {
        match column {
            DataColumnarValue::Constant(value, _) => Ok(value),
            DataColumnarValue::Array(array) => {
                if let Ok(v) = dispatch_primitive_array! { typed_array_op_to_data_value, array, max}
                {
                    Ok(v)
                } else {
                    dispatch_string_array! {typed_string_array_op_to_data_value, array, max_string}
                }
            }
        }
    }
}
