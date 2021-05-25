// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregateOperator;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::IAggregateFunction;

#[derive(Clone)]
pub struct AggregateSumFunction {
    display_name: String,
    depth: usize,
    state: DataValue
}

impl AggregateSumFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IAggregateFunction>> {
        Ok(Box::new(AggregateSumFunction {
            display_name: display_name.to_string(),
            depth: 0,
            state: DataValue::Null
        }))
    }
}

impl IAggregateFunction for AggregateSumFunction {
    fn name(&self) -> &str {
        "AggregateSumFunction"
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
        self.state = DataValueArithmetic::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Sum,
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
        self.state = DataValueArithmetic::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            val
        )?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateSumFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
