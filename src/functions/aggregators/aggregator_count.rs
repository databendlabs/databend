// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    self, DataColumnarValue, DataSchema, DataType, DataValue, DataValueArithmeticOperator,
};
use crate::error::FuseQueryResult;
use crate::functions::{ConstantFunction, IFunction};
use crate::sessions::FuseQueryContextRef;

#[derive(Clone)]
pub struct AggregatorCountFunction {
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorCountFunction {
    pub fn try_create(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        let arg = if args.is_empty() {
            ConstantFunction::try_create(DataValue::UInt64(Some(1)))?
        } else {
            args[0].clone()
        };
        Ok(Box::new(AggregatorCountFunction {
            depth: 0,
            arg,
            state: DataValue::Null,
        }))
    }
}

impl IFunction for AggregatorCountFunction {
    fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.arg.return_type(input_schema)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        self.arg.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        let rows = block.num_rows();
        self.state = datavalues::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            DataValue::UInt64(Some(rows as u64)),
        )?;
        Ok(())
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        let val = states[self.depth].clone();
        self.state = datavalues::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Ok(self.state.clone())
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Count({})", self.arg)
    }
}
