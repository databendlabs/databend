// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    array_max, array_sum, datavalue_add, datavalue_max, DataArrayRef, DataSchema, DataType,
    DataValue,
};
use crate::error::{Error, Result};
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct CountAggregatorFunction {
    name: &'static str,
    column: Arc<Function>,
    state: DataValue,
    data_type: DataType,
}

#[derive(Clone, Debug)]
pub struct MaxAggregatorFunction {
    name: &'static str,
    column: Arc<Function>,
    state: DataValue,
    data_type: DataType,
}

#[derive(Clone, Debug)]
pub struct SumAggregatorFunction {
    name: &'static str,
    column: Arc<Function>,
    state: DataValue,
    data_type: DataType,
}

#[derive(Clone, Debug)]
pub enum AggregatorFunction {
    Count(CountAggregatorFunction),
    Max(MaxAggregatorFunction),
    Sum(SumAggregatorFunction),
}

impl AggregatorFunction {
    pub fn create(name: &str, column: Arc<Function>, data_type: &DataType) -> Result<Function> {
        Ok(Function::Aggregator(match name.to_lowercase().as_str() {
            "count" => AggregatorFunction::Count(CountAggregatorFunction {
                name: "CountAggregatorFunction",
                column,
                state: DataValue::try_from(&DataType::UInt64)?,
                data_type: DataType::UInt64,
            }),
            "max" => AggregatorFunction::Max(MaxAggregatorFunction {
                name: "MaxAggregatorFunction",
                column,
                state: DataValue::try_from(data_type)?,
                data_type: data_type.clone(),
            }),
            "sum" => AggregatorFunction::Sum(SumAggregatorFunction {
                name: "SumAggregatorFunction",
                column,
                state: DataValue::try_from(data_type)?,
                data_type: data_type.clone(),
            }),
            _ => {
                return Err(Error::Unsupported(format!(
                    "Unsupported aggregators function: {:?}",
                    name
                )))
            }
        }))
    }

    pub fn name(&self) -> &'static str {
        match self {
            AggregatorFunction::Count(v) => v.name,
            AggregatorFunction::Max(v) => v.name,
            AggregatorFunction::Sum(v) => v.name,
        }
    }

    pub fn return_type(&self) -> Result<DataType> {
        Ok(match self {
            AggregatorFunction::Count(v) => v.data_type.clone(),
            AggregatorFunction::Max(v) => v.data_type.clone(),
            AggregatorFunction::Sum(v) => v.data_type.clone(),
        })
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    // Accumulates a value.
    pub fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        match self {
            AggregatorFunction::Count(v) => {
                let array = v.column.evaluate(block)?.len();
                v.state = datavalue_add(v.state.clone(), DataValue::UInt64(Some(array as u64)))?;
            }
            AggregatorFunction::Max(v) => {
                let array = v.column.evaluate(block)?;
                v.state = datavalue_max(v.state.clone(), array_max(array)?)?;
            }
            AggregatorFunction::Sum(v) => {
                let array = v.column.evaluate(block)?;
                v.state = datavalue_add(v.state.clone(), array_sum(array)?)?;
            }
        }
        Ok(())
    }

    // Calculates a final aggregators.
    pub fn aggregate(&self) -> Result<DataArrayRef> {
        Ok(match self {
            AggregatorFunction::Count(v) => v.state.to_array()?,
            AggregatorFunction::Max(v) => v.state.to_array()?,
            AggregatorFunction::Sum(v) => v.state.to_array()?,
        })
    }
}

impl fmt::Display for AggregatorFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
