// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::ensure;
use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayAggregate;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregate;
use common_datavalues::DataValueAggregateOperator;

use crate::IFunction;

#[derive(Clone)]
pub struct AggregatorMaxFunction {
    display_name: String,
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue
}

impl AggregatorMaxFunction {
    pub fn try_create(
        display_name: &str,
        args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {
        ensure!(
            args.len() == 1,
            "Function Error: Aggregator function {} args require single argument",
            display_name
        );

        Ok(Box::new(AggregatorMaxFunction {
            display_name: display_name.to_string(),
            depth: 0,
            arg: args[0].clone(),
            state: DataValue::Null
        }))
    }
}

impl IFunction for AggregatorMaxFunction {
    fn name(&self) -> &str {
        "AggregatorMaxFunction"
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
        self.state = DataValueAggregate::data_value_aggregate_op(
            DataValueAggregateOperator::Max,
            self.state.clone(),
            DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Max,
                val.to_array(rows)?
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
            DataValueAggregateOperator::Max,
            self.state.clone(),
            val
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

impl fmt::Display for AggregatorMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.display_name, self.arg)
    }
}
