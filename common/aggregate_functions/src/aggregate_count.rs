// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::aggregator_common::assert_variadic_arguments;
use crate::AggregateFunction;

#[derive(Clone)]
pub struct AggregateCountFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateCountFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Box::new(AggregateCountFunction {
            display_name: display_name.to_string(),
            state: DataValue::UInt64(Some(0)),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateCountFunction {
    fn name(&self) -> &str {
        "AggregateCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulate(&mut self, _columns: &[DataColumn], input_rows: usize) -> Result<()> {
        self.state = (&self.state + &DataValue::UInt64(Some(input_rows as u64)))?;

        Ok(())
    }

    fn accumulate_scalar(&mut self, _values: &[DataValue]) -> Result<()> {
        self.state = (&self.state + &DataValue::UInt64(Some(1u64)))?;
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let value = states[0].clone();
        self.state = (&self.state + &value)?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
