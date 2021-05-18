// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::sync::Arc;

use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregate;
use common_datavalues::DataValueAggregateOperator;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IAggreagteFunction;

#[derive(Clone)]
pub struct AggregateMinFunction {
    display_name: String,
    depth: usize,
    state: DataValue
}

impl AggregateMinFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IAggreagteFunction>> {
        Ok(Box::new(AggregateMinFunction {
            display_name: display_name.to_string(),
            depth: 0,
            state: DataValue::Null
        }))
    }
}

impl IAggreagteFunction for AggregateMinFunction {
    fn name(&self) -> &str {
        "AggregateMinFunction"
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

    fn accumulate(&mut self, columns: &[DataColumnarValue], input_rows: usize) -> Result<()> {
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Min,
                columns[0].to_array()?
            )?
        )?;
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[self.depth].clone();
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            val
        )?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
