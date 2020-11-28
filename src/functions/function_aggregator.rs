// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::convert::TryFrom;
use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    data_array_aggregate_op, data_value_add, data_value_aggregate_op, DataColumnarValue,
    DataSchema, DataType, DataValue, DataValueAggregateOperator,
};
use crate::error::FuseQueryResult;
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct AggregatorFunction {
    op: DataValueAggregateOperator,
    arg: Box<Function>,
    state: DataValue,
}

impl AggregatorFunction {
    pub fn try_create(
        op: DataValueAggregateOperator,
        args: &[Function],
    ) -> FuseQueryResult<Function> {
        let mut state = DataValue::Null;
        if op == DataValueAggregateOperator::Count {
            state = DataValue::try_from(&DataType::UInt64)?;
        }

        Ok(Function::Aggregator(AggregatorFunction {
            op,
            arg: Box::new(args[0].clone()),
            state,
        }))
    }

    pub fn name(&self) -> &'static str {
        match self.op {
            DataValueAggregateOperator::Min => "MinAggregatorFunction",
            DataValueAggregateOperator::Max => "MaxAggregatorFunction",
            DataValueAggregateOperator::Sum => "SumAggregatorFunction",
            DataValueAggregateOperator::Count => "CountAggregatorFunction",
        }
    }

    pub fn return_type(&self) -> FuseQueryResult<DataType> {
        Ok(self.state.data_type())
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    // Accumulates a value.
    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        let rows = block.num_rows();
        self.arg.eval(block)?;
        let val = self.arg.result()?;
        match &self.op {
            DataValueAggregateOperator::Count => {
                self.state = data_value_add(
                    self.state.clone(),
                    DataValue::UInt64(Some(val.to_array(rows)?.len() as u64)),
                )?;
            }
            DataValueAggregateOperator::Min => {
                self.state = data_value_aggregate_op(
                    DataValueAggregateOperator::Min,
                    self.state.clone(),
                    data_array_aggregate_op(DataValueAggregateOperator::Min, val.to_array(rows)?)?,
                )?;
            }
            DataValueAggregateOperator::Max => {
                self.state = data_value_aggregate_op(
                    DataValueAggregateOperator::Max,
                    self.state.clone(),
                    data_array_aggregate_op(DataValueAggregateOperator::Max, val.to_array(rows)?)?,
                )?;
            }
            DataValueAggregateOperator::Sum => {
                self.state = data_value_add(
                    self.state.clone(),
                    data_array_aggregate_op(DataValueAggregateOperator::Sum, val.to_array(rows)?)?,
                )?;
            }
        }
        Ok(())
    }

    // Calculates a final aggregators.
    pub fn result(&self) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(self.state.clone()))
    }
}

impl fmt::Display for AggregatorFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}({:?})", self.op, self.arg)
    }
}
