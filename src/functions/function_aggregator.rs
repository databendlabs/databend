// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    data_array_aggregate_op, data_value_add, data_value_aggregate_op, DataSchema, DataType,
    DataValue, DataValueAggregateOperator,
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
        let rows = block.num_rows();
        match self {
            AggregatorFunction::Count(v) => {
                let val = v.column.evaluate(block)?;
                v.state = data_value_add(
                    v.state.clone(),
                    DataValue::UInt64(Some(val.to_array(rows)?.len() as u64)),
                )?;
            }
            AggregatorFunction::Min(v) => {
                let val = v.column.evaluate(block)?;
                v.state = data_value_aggregate_op(
                    DataValueAggregateOperator::Min,
                    v.state.clone(),
                    data_array_aggregate_op(DataValueAggregateOperator::Min, val.to_array(rows)?)?,
                )?;
            }
            AggregatorFunction::Max(v) => {
                let val = v.column.evaluate(block)?;
                v.state = data_value_aggregate_op(
                    DataValueAggregateOperator::Max,
                    v.state.clone(),
                    data_array_aggregate_op(DataValueAggregateOperator::Max, val.to_array(rows)?)?,
                )?;
            }
            AggregatorFunction::Sum(v) => {
                let val = v.column.evaluate(block)?;
                v.state = data_value_add(
                    v.state.clone(),
                    data_array_aggregate_op(DataValueAggregateOperator::Sum, val.to_array(rows)?)?,
                )?;
            }
        }
        Ok(())
    }

    // Calculates a final aggregators.
    pub fn aggregate(&self) -> FuseQueryResult<DataValue> {
        Ok(match self {
            AggregatorFunction::Count(v) => v.state.clone(),
            AggregatorFunction::Min(v) => v.state.clone(),
            AggregatorFunction::Max(v) => v.state.clone(),
            AggregatorFunction::Sum(v) => v.state.clone(),
        })
    }
}

impl fmt::Display for AggregatorFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
