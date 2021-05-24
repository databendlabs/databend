// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataArrayAggregate;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregateOperator;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct AggregatorAvgFunction {
    display_name: String,
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorAvgFunction {
    pub fn try_create(
        display_name: &str,
        args: &[Box<dyn IFunction>],
    ) -> Result<Box<dyn IFunction>> {
        match args.len() {
            1 => Ok(Box::new(AggregatorAvgFunction {
                display_name: display_name.to_string(),
                depth: 0,
                arg: args[0].clone(),
                state: DataValue::Struct(vec![DataValue::Null, DataValue::UInt64(Some(0))]),
            })),
            _ => Result::Err(ErrorCodes::BadArguments(format!(
                "Function Error: Aggregator function {} args require single argument",
                display_name
            ))),
        }
    }
}

impl IFunction for AggregatorAvgFunction {
    fn name(&self) -> &str {
        "AggregatorAvgFunction"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;

        if let DataValue::Struct(values) = self.state.clone() {
            let sum = DataValueArithmetic::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                values[0].clone(),
                DataArrayAggregate::data_array_aggregate_op(
                    DataValueAggregateOperator::Sum,
                    val.to_array(1)?,
                )?,
            )?;
            let count = DataValueArithmetic::data_value_arithmetic_op(
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
            let sum = DataValueArithmetic::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                new_states[0].clone(),
                old_states[0].clone(),
            )?;
            let count = DataValueArithmetic::data_value_arithmetic_op(
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
            DataValueArithmetic::data_value_arithmetic_op(
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
        write!(f, "{}({})", self.display_name, self.arg)
    }
}
