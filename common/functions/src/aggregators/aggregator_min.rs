// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::bail;
use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregateOperator;
use common_datavalues::{self as datavalues};

use crate::IFunction;

#[derive(Clone)]
pub struct AggregatorMinFunction {
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorMinFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        if args.len() != 1 {
            bail!("Function Error: Aggregator function Min args require single argument");
        }

        Ok(Box::new(AggregatorMinFunction {
            depth: 0,
            arg: args[0].clone(),
            state: DataValue::Null,
        }))
    }
}

impl IFunction for AggregatorMinFunction {
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
        self.state = datavalues::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            datavalues::data_array_aggregate_op(
                DataValueAggregateOperator::Min,
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
        self.state = datavalues::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
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

impl fmt::Display for AggregatorMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "min({})", self.arg)
    }
}
