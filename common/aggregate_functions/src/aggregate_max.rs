// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregate;
use common_datavalues::DataValueAggregateOperator;
use common_exception::Result;

use crate::IAggregateFunction;

#[derive(Clone)]
pub struct AggregateMaxFunction {
    display_name: String,
    depth: usize,
    state: DataValue
}

impl AggregateMaxFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IAggregateFunction>> {
        Ok(Box::new(AggregateMaxFunction {
            display_name: display_name.to_string(),
            depth: 0,
            state: DataValue::Null
        }))
    }
}

impl IAggregateFunction for AggregateMaxFunction {
    fn name(&self) -> &str {
        "AggregateMaxFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<()> {
        let value = match &columns[0] {
            DataColumnarValue::Array(array) => DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Max,
                array.clone()
            ),
            DataColumnarValue::Constant(s, _) => Ok(s.clone())
        }?;

        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Max,
            self.state.clone(),
            value
        )?;

        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[self.depth].clone();
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Max,
            self.state.clone(),
            val
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
