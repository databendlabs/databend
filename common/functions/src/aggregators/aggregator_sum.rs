// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_exception::{Result, ErrorCodes};
use common_datablocks::DataBlock;
use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregateOperator;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;

use crate::IFunction;

#[derive(Clone)]
pub struct AggregatorSumFunction {
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue
}

impl AggregatorSumFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        match args.len() {
            1 => {
                Ok(Box::new(AggregatorSumFunction {
                    depth: 0,
                    arg: args[0].clone(),
                    state: DataValue::Null,
                }))
            }
            _ => Result::Err(ErrorCodes::BadArguments("Function Error: Aggregator function Sum args require single argument".to_string()))
        }
    }
}

impl IFunction for AggregatorSumFunction {
    fn name(&self) -> &str {
        "AggregatorSumFunction"
    }

    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        self.arg.return_type(input_schema)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        self.arg.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;

        self.state = DataValueArithmetic::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Sum,
                val.to_array(rows)?,
            )?,
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
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorSumFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "sum({})", self.arg)
    }
}
