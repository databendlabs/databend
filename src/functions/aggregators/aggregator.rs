// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    data_array_max, data_array_min, data_array_sum, data_value_add, data_value_max, data_value_min,
    DataArrayRef, DataSchema, DataType, DataValue,
};
use crate::error::{FuseQueryError, FuseQueryResult};
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
pub struct MinAggregatorFunction {
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
    Min(MinAggregatorFunction),
    Max(MaxAggregatorFunction),
    Sum(SumAggregatorFunction),
}

impl AggregatorFunction {
    pub fn create(
        name: &str,
        column: Arc<Function>,
        data_type: &DataType,
    ) -> FuseQueryResult<Function> {
        Ok(Function::Aggregator(match name.to_lowercase().as_str() {
            "count" => AggregatorFunction::Count(CountAggregatorFunction {
                name: "CountAggregatorFunction",
                column,
                state: DataValue::try_from(&DataType::UInt64)?,
                data_type: DataType::UInt64,
            }),
            "min" => AggregatorFunction::Min(MinAggregatorFunction {
                name: "MinAggregatorFunction",
                column,
                state: DataValue::try_from(data_type)?,
                data_type: data_type.clone(),
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
                return Err(FuseQueryError::Unsupported(format!(
                    "Unsupported aggregators function: {:?}",
                    name
                )))
            }
        }))
    }

    pub fn name(&self) -> &'static str {
        match self {
            AggregatorFunction::Count(v) => v.name,
            AggregatorFunction::Min(v) => v.name,
            AggregatorFunction::Max(v) => v.name,
            AggregatorFunction::Sum(v) => v.name,
        }
    }

    pub fn return_type(&self) -> FuseQueryResult<DataType> {
        Ok(match self {
            AggregatorFunction::Count(v) => v.data_type.clone(),
            AggregatorFunction::Min(v) => v.data_type.clone(),
            AggregatorFunction::Max(v) => v.data_type.clone(),
            AggregatorFunction::Sum(v) => v.data_type.clone(),
        })
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    // Accumulates a value.
    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        match self {
            AggregatorFunction::Count(v) => {
                let array = v.column.evaluate(block)?.len();
                v.state = data_value_add(v.state.clone(), DataValue::UInt64(Some(array as u64)))?;
            }
            AggregatorFunction::Min(v) => {
                let array = v.column.evaluate(block)?;
                v.state = data_value_min(v.state.clone(), data_array_min(array)?)?;
            }
            AggregatorFunction::Max(v) => {
                let array = v.column.evaluate(block)?;
                v.state = data_value_max(v.state.clone(), data_array_max(array)?)?;
            }
            AggregatorFunction::Sum(v) => {
                let array = v.column.evaluate(block)?;
                v.state = data_value_add(v.state.clone(), data_array_sum(array)?)?;
            }
        }
        Ok(())
    }

    // Calculates a final aggregators.
    pub fn aggregate(&self) -> FuseQueryResult<DataArrayRef> {
        Ok(match self {
            AggregatorFunction::Count(v) => v.state.to_array()?,
            AggregatorFunction::Min(v) => v.state.to_array()?,
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
