// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::{FactoryFuncRef, UDFExampleFunction};

#[derive(Clone, Debug)]
pub enum UDFFunction {
    UDFExample(UDFExampleFunction),
}

impl UDFFunction {
    pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("example", UDFExampleFunction::try_create);
        Ok(())
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        match self {
            UDFFunction::UDFExample(v) => v.return_type(input_schema),
        }
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        match self {
            UDFFunction::UDFExample(v) => v.nullable(input_schema),
        }
    }

    pub fn set_depth(&mut self, depth: usize) {
        match self {
            UDFFunction::UDFExample(v) => v.set_depth(depth),
        }
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        match self {
            UDFFunction::UDFExample(v) => v.eval(block),
        }
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        match self {
            UDFFunction::UDFExample(v) => v.accumulate(block),
        }
    }

    pub fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        match self {
            UDFFunction::UDFExample(v) => v.accumulate_result(),
        }
    }

    pub fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        match self {
            UDFFunction::UDFExample(v) => v.merge(states),
        }
    }

    pub fn merge_result(&self) -> FuseQueryResult<DataValue> {
        match self {
            UDFFunction::UDFExample(v) => v.merge_result(),
        }
    }
}

impl fmt::Display for UDFFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UDFFunction::UDFExample(v) => write!(f, "{}", v),
        }
    }
}
