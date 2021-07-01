// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::aggregator_common::assert_unary_arguments;
use crate::AggregateFunction;

#[derive(Clone)]
pub struct AggregateSumFunction {
    display_name: String,
    state: DataValue,
    arguments: Vec<DataField>,
}

impl AggregateSumFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;

        let return_type = Self::sum_return_type(arguments[0].data_type())?;
        Ok(Box::new(AggregateSumFunction {
            display_name: display_name.to_string(),
            state: DataValue::from(&return_type),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateSumFunction {
    fn name(&self) -> &str {
        "AggregateSumFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.state.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulate(&mut self, columns: &[DataColumn], _input_rows: usize) -> Result<()> {
        let value = Self::sum_batch(&columns[0])?;
        self.state = (&self.state + &value)?;
        Ok(())
    }

    fn accumulate_scalar(&mut self, values: &[DataValue]) -> Result<()> {
        self.state = (&self.state + &values[0])?;

        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[0].clone();
        self.state = (&self.state + &val)?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregateSumFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateSumFunction {
    fn sum_return_type(arg_type: &DataType) -> Result<DataType> {
        match arg_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(DataType::UInt64)
            }
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Float64 => Ok(DataType::Float64),

            other => Err(ErrorCode::BadDataValueType(format!(
                "SUM does not support type '{:?}'",
                other
            ))),
        }
    }

    pub fn sum_batch(column: &DataColumn) -> Result<DataValue> {
        match column {
            DataColumn::Constant(value, size) => {
                DataValue::arithmetic(Mul, value.clone(), DataValue::UInt64(Some(*size as u64)))
            }
            DataColumn::Array(array) => array.sum(),
        }
    }
}
