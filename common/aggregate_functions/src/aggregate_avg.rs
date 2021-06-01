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
pub struct AggregateAvgFunction {
    display_name: String,
    depth: usize,
    state: DataValue,
}

impl AggregateAvgFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IAggregateFunction>> {
        Ok(Box::new(AggregateAvgFunction {
            display_name: display_name.to_string(),
            depth: 0,
            state: DataValue::Struct(vec![DataValue::Null, DataValue::UInt64(Some(0))]),
        }))
    }
}

impl IAggregateFunction for AggregateAvgFunction {
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, columns: &[DataColumnarValue], input_rows: usize) -> Result<()> {
        if let DataValue::Struct(values) = self.state.clone() {
            let sum = DataValueArithmetic::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                values[0].clone(),
                DataArrayAggregate::data_array_aggregate_op(
                    DataValueAggregateOperator::Sum,
                    columns[0].to_array()?,
                )?,
            )?;
            let count = DataValueArithmetic::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                values[1].clone(),
                DataValue::UInt64(Some(input_rows as u64)),
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
}

impl fmt::Display for AggregateAvgFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
