// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::IFunction;

#[derive(Clone)]
pub struct AliasFunction {
    depth: usize,
    alias: String,
    func: Box<dyn IFunction>,
}

impl AliasFunction {
    pub fn try_create(
        alias: String,
        func: Box<dyn IFunction>,
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        Ok(Box::new(AliasFunction {
            depth: 0,
            alias,
            func,
        }))
    }
}

impl IFunction for AliasFunction {
    fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.func.return_type(input_schema)
    }

    fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        self.func.nullable(input_schema)
    }

    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        self.func.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.func.accumulate(block)
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        self.func.accumulate_result()
    }

    fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        self.func.merge(states)
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        self.func.merge_result()
    }

    fn is_aggregator(&self) -> bool {
        self.func.is_aggregator()
    }
}

impl fmt::Display for AliasFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.alias)
    }
}
