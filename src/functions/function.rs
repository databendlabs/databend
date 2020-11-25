// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{
    AggregatorFunction, ArithmeticFunction, ComparisonFunction, ConstantFunction, VariableFunction,
};

#[derive(Clone)]
pub enum Function {
    Constant(ConstantFunction),
    Variable(VariableFunction),
    Arithmetic(ArithmeticFunction),
    Comparison(ComparisonFunction),
    Aggregator(AggregatorFunction),
}

impl Function {
    pub fn name(&self) -> &'static str {
        match self {
            Function::Constant(v) => v.name(),
            Function::Variable(v) => v.name(),
            Function::Arithmetic(v) => v.name(),
            Function::Comparison(v) => v.name(),
            Function::Aggregator(v) => v.name(),
        }
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        match self {
            Function::Constant(v) => v.return_type(input_schema),
            Function::Variable(v) => v.return_type(input_schema),
            Function::Arithmetic(v) => v.return_type(input_schema),
            Function::Comparison(v) => v.return_type(input_schema),
            Function::Aggregator(v) => v.return_type(),
        }
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool> {
        match self {
            Function::Constant(v) => v.nullable(input_schema),
            Function::Variable(v) => v.nullable(input_schema),
            Function::Arithmetic(v) => v.nullable(input_schema),
            Function::Comparison(v) => v.nullable(input_schema),
            Function::Aggregator(v) => v.nullable(input_schema),
        }
    }

    pub fn evaluate(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        match self {
            Function::Constant(v) => v.evaluate(block),
            Function::Variable(v) => v.evaluate(block),
            Function::Arithmetic(v) => v.evaluate(block),
            Function::Comparison(v) => v.evaluate(block),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported evaluate() for function {}",
                self.name()
            ))),
        }
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        match self {
            Function::Aggregator(ref mut v) => v.accumulate(block),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported accumulate() for function {}",
                self.name()
            ))),
        }
    }

    pub fn aggregate(&self) -> FuseQueryResult<DataValue> {
        match self {
            Function::Aggregator(v) => v.aggregate(),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported aggregators() for function {}",
                self.name()
            ))),
        }
    }
}

impl fmt::Debug for Function {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Function::Constant(v) => write!(f, "{}", v),
            Function::Variable(v) => write!(f, "{}", v),
            Function::Arithmetic(v) => write!(f, "{}", v),
            Function::Comparison(v) => write!(f, "{}", v),
            Function::Aggregator(v) => write!(f, "{}", v),
        }
    }
}
