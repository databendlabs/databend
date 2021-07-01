// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::aggregator_common::assert_unary_arguments;
use crate::AggregateFunction;

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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulate(&mut self, columns: &[DataColumn], input_rows: usize) -> Result<()> {
        if let DataValue::Struct(values) = self.state.clone() {
            let sum = match &columns[0] {
                DataColumn::Constant(value, size) => {
                    DataValue::arithmetic(Mul, value.clone(), DataValue::UInt64(Some(*size as u64)))
                }
                DataColumn::Array(array) => array.sum(),
            }?;

            let sum = (&sum + &values[0])?;

            let count = DataValue::UInt64(Some(input_rows as u64));
            let count = (&count + &values[1])?;

            self.state = DataValue::Struct(vec![sum, count]);
        }
        Ok(())
    }

    fn accumulate_scalar(&mut self, scalar_values: &[DataValue]) -> Result<()> {
        if let DataValue::Struct(values) = self.state.clone() {
            let sum = (&scalar_values[0] + &values[0])?;
            let count = DataValue::UInt64(Some(1_u64));
            let count = (&count + &values[1])?;

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
            let sum = (&new_states[0] + &old_states[0])?;
            let count = (&new_states[1] + &old_states[1])?;

            self.state = DataValue::Struct(vec![sum, count]);
        }
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(if let DataValue::Struct(states) = self.state.clone() {
            if states[1].eq(&DataValue::UInt64(Some(0))) {
                DataValue::Float64(None)
            } else {
                (&states[0] / &states[1])?
            }
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
