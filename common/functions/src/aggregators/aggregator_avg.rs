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
use common_datavalues::DataValueArithmeticOperator;
use common_datavalues::{self as datavalues};

use crate::IFunction;

#[derive(Clone)]
pub struct AggregatorAvgFunction {
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorAvgFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        if args.len() != 1 {
            bail!(
                "Function Error: Aggregator function Avg args require single argument".to_string(),
            );
        }

        Ok(Box::new(AggregatorAvgFunction {
            depth: 0,
            arg: args[0].clone(),
            state: DataValue::Struct(vec![DataValue::Null, DataValue::UInt64(Some(0))]),
        }))
    }
}

impl IFunction for AggregatorAvgFunction {
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

        if let DataValue::Struct(values) = self.state.clone() {
            let sum = datavalues::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                values[0].clone(),
                datavalues::data_array_aggregate_op(
                    DataValueAggregateOperator::Sum,
                    val.to_array(1)?,
                )?,
            )?;
            let count = datavalues::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                values[1].clone(),
                DataValue::UInt64(Some(rows as u64)),
            )?;

            self.state = DataValue::Struct(vec![sum, count]);
        }
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[self.depth].clone();
        if let (DataValue::Struct(new_states), DataValue::Struct(old_states)) =
            (val, self.state.clone())
        {
            let sum = datavalues::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                new_states[0].clone(),
                old_states[0].clone(),
            )?;
            let count = datavalues::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                new_states[1].clone(),
                old_states[1].clone(),
            )?;
            self.state = DataValue::Struct(vec![sum, count]);
        }
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(if let DataValue::Struct(states) = self.state.clone() {
            datavalues::data_value_arithmetic_op(
                DataValueArithmeticOperator::Div,
                states[0].clone(),
                states[1].clone(),
            )?
        } else {
            self.state.clone()
        })
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorAvgFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "avg({})", self.arg)
    }
}
