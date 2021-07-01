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
pub struct AggregateMinFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateMinFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;

        Ok(Box::new(AggregateMinFunction {
            display_name: display_name.to_string(),
            state: DataValue::from(arguments[0].data_type()),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateMinFunction {
    fn name(&self) -> &str {
        "AggregateMinFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulate(&mut self, columns: &[DataColumn], _input_rows: usize) -> Result<()> {
        let value = Self::min_batch(&columns[0])?;

        self.state = DataValue::agg(Min, self.state.clone(), value)?;
        Ok(())
    }

    fn accumulate_scalar(&mut self, values: &[DataValue]) -> Result<()> {
        self.state = DataValue::agg(Min, self.state.clone(), values[0].clone())?;

        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let value = states[0].clone();
        self.state = DataValue::agg(Min, self.state.clone(), value)?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateMinFunction {
    pub fn min_batch(column: &DataColumn) -> Result<DataValue> {
        match column {
            DataColumn::Constant(value, _) => Ok(value.clone()),
            DataColumn::Array(array) => array.min(),
        }
    }
}
