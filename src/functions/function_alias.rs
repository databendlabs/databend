// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::Function;

#[derive(Clone, Debug)]
pub struct AliasFunction {
    alias: String,
    func: Box<Function>,
}

impl AliasFunction {
    pub fn try_create(alias: String, func: Function) -> FuseQueryResult<Function> {
        Ok(Function::Alias(AliasFunction {
            alias,
            func: Box::new(func),
        }))
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.func.return_type(input_schema)
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        self.func.nullable(input_schema)
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.func.eval(block)
    }

    pub fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        self.func.merge(states)
    }

    pub fn state(&self) -> FuseQueryResult<Vec<DataValue>> {
        self.func.state()
    }

    pub fn result(&self) -> FuseQueryResult<DataColumnarValue> {
        self.func.result()
    }
}

impl fmt::Display for AliasFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.alias)
    }
}
