// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues;
use crate::datavalues::{
    DataColumnarValue, DataSchema, DataType, DataValue, DataValueAggregateOperator,
    DataValueArithmeticOperator,
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
        let state = DataValue::Null;
        Ok(Function::Aggregator(AggregatorFunction {
            op,
            arg: Box::new(args[0].clone()),
            state,
        }))
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        match self.op {
            DataValueAggregateOperator::Count => Ok(DataType::UInt64),
            _ => self.arg.return_type(input_schema),
        }
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        self.arg.eval(block)
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;
        match &self.op {
            DataValueAggregateOperator::Count => {
                self.state = datavalues::data_value_arithmetic_op(
                    DataValueArithmeticOperator::Add,
                    self.state.clone(),
                    DataValue::UInt64(Some(rows as u64)),
                )?;
            }
            DataValueAggregateOperator::Min => {
                self.state = datavalues::data_value_aggregate_op(
                    DataValueAggregateOperator::Min,
                    self.state.clone(),
                    datavalues::data_array_aggregate_op(
                        DataValueAggregateOperator::Min,
                        val.to_array(rows)?,
                    )?,
                )?;
            }
            DataValueAggregateOperator::Max => {
                self.state = datavalues::data_value_aggregate_op(
                    DataValueAggregateOperator::Max,
                    self.state.clone(),
                    datavalues::data_array_aggregate_op(
                        DataValueAggregateOperator::Max,
                        val.to_array(rows)?,
                    )?,
                )?;
            }
            DataValueAggregateOperator::Sum => {
                self.state = datavalues::data_value_arithmetic_op(
                    DataValueArithmeticOperator::Add,
                    self.state.clone(),
                    datavalues::data_array_aggregate_op(
                        DataValueAggregateOperator::Sum,
                        val.to_array(rows)?,
                    )?,
                )?;
            }
        }
        Ok(())
    }

    pub fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    pub fn merge_state(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        let val = states[0].clone();
        match &self.op {
            DataValueAggregateOperator::Count => {
                self.state = datavalues::data_value_arithmetic_op(
                    DataValueArithmeticOperator::Add,
                    self.state.clone(),
                    val,
                )?;
            }
            DataValueAggregateOperator::Min => {
                self.state = datavalues::data_value_aggregate_op(
                    DataValueAggregateOperator::Min,
                    self.state.clone(),
                    val,
                )?;
            }
            DataValueAggregateOperator::Max => {
                self.state = datavalues::data_value_aggregate_op(
                    DataValueAggregateOperator::Max,
                    self.state.clone(),
                    val,
                )?;
            }
            DataValueAggregateOperator::Sum => {
                self.state = datavalues::data_value_arithmetic_op(
                    DataValueArithmeticOperator::Add,
                    self.state.clone(),
                    val,
                )?;
            }
        }
        Ok(())
    }

    pub fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Ok(self.state.clone())
    }
}

impl fmt::Display for AggregatorFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}({:?})", self.op, self.arg)
    }
}
