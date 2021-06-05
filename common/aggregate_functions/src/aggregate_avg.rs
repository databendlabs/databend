// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataColumnarValue;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::aggregator_common::assert_unary_arguments;
use crate::AggregateFunction;
use crate::AggregateSumFunction;

#[derive(Clone)]
pub struct AggregateAvgFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateAvgFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;

        Ok(Box::new(AggregateAvgFunction {
            display_name: display_name.to_string(),
            state: DataValue::Struct(vec![DataValue::Null, DataValue::UInt64(Some(0))]),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateAvgFunction {
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn accumulate(&mut self, columns: &[DataColumnarValue], input_rows: usize) -> Result<()> {
        if let DataValue::Struct(values) = self.state.clone() {
            let sum = DataValueArithmetic::data_value_arithmetic_op(
                DataValueArithmeticOperator::Plus,
                values[0].clone(),
                AggregateSumFunction::sum_batch(columns[0].clone())?,
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
        let val = states[0].clone();
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
