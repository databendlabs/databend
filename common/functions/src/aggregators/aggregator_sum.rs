// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::{
    self as datavalues, DataColumnarValue, DataSchema, DataType, DataValue,
    DataValueAggregateOperator, DataValueArithmeticOperator,
};

use crate::IFunction;
use crate::{FunctionError, FunctionResult};

#[derive(Clone)]
pub struct AggregatorSumFunction {
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorSumFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        if args.len() != 1 {
            return Err(FunctionError::build_internal_error(
                "Aggregator function Sum args require single argument".to_string(),
            ));
        }
        Ok(Box::new(AggregatorSumFunction {
            depth: 0,
            arg: args[0].clone(),
            state: DataValue::Null,
        }))
    }
}

impl IFunction for AggregatorSumFunction {
    fn return_type(&self, input_schema: &DataSchema) -> FunctionResult<DataType> {
        self.arg.return_type(input_schema)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FunctionResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FunctionResult<DataColumnarValue> {
        self.arg.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> FunctionResult<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;

        self.state = datavalues::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            datavalues::data_array_aggregate_op(
                DataValueAggregateOperator::Sum,
                val.to_array(rows)?,
            )?,
        )?;

        Ok(())
    }

    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> FunctionResult<()> {
        let val = states[self.depth].clone();
        self.state = datavalues::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> FunctionResult<DataValue> {
        Ok(self.state.clone())
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorSumFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Sum({})", self.arg)
    }
}
