// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datatypes::{DataArrayRef, DataSchema, DataType, DataValue};
use crate::error::Result;

use crate::functions::Function;

#[derive(Clone, Debug, PartialEq)]
pub struct CountAggregateFunction {
    count: u64,
}

impl CountAggregateFunction {
    pub fn create() -> Result<Function> {
        Ok(Function::Count(CountAggregateFunction { count: 0 }))
    }

    pub fn name(&self) -> &'static str {
        "CountAggregateFunction"
    }

    pub fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    // Accumulates a value.
    pub fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        self.count += block.rows();
        Ok(())
    }

    // Calculates a final aggregate.
    pub fn aggregate(&self) -> Result<DataArrayRef> {
        DataValue::UInt64(self.count).to_array()
    }
}

impl fmt::Display for CountAggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
