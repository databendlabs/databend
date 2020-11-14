// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datavalues::{array_max, datavalue_max, DataArrayRef, DataSchema, DataType, DataValue};
use crate::error::Result;
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct MaxAggregateFunction {
    column: Arc<Function>,
    max: DataValue,
    data_type: DataType,
}

impl MaxAggregateFunction {
    pub fn create(column: Arc<Function>, data_type: &DataType) -> Result<Function> {
        Ok(Function::Max(MaxAggregateFunction {
            column,
            max: DataValue::try_from(data_type)?,
            data_type: data_type.clone(),
        }))
    }

    pub fn name(&self) -> &'static str {
        "MaxAggregateFunction"
    }

    pub fn return_type(&self) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    // Accumulates a value.
    pub fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        let array = self.column.evaluate(block)?;
        self.max = datavalue_max(self.max.clone(), array_max(array)?)?;
        Ok(())
    }

    // Calculates a final aggregate.
    pub fn aggregate(&self) -> Result<DataArrayRef> {
        self.max.to_array()
    }
}

impl fmt::Display for MaxAggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
