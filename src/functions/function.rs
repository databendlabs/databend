// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataArrayRef, DataSchema, DataType};
use crate::error::{Error, Result};

use crate::functions::{aggregate, arithmetic, ConstantFunction, VariableFunction};

#[derive(Clone)]
pub enum Function {
    Constant(ConstantFunction),
    Variable(VariableFunction),
    Add(arithmetic::AddFunction),
    Sub(arithmetic::SubFunction),
    Div(arithmetic::DivFunction),
    Mul(arithmetic::MulFunction),
    Count(aggregate::CountAggregateFunction),
    Sum(aggregate::SumAggregateFunction),
    Max(aggregate::MaxAggregateFunction),
}

impl Function {
    pub fn name(&self) -> &'static str {
        match self {
            Function::Constant(v) => v.name(),
            Function::Variable(v) => v.name(),
            Function::Add(v) => v.name(),
            Function::Div(v) => v.name(),
            Function::Mul(v) => v.name(),
            Function::Sub(v) => v.name(),
            Function::Count(v) => v.name(),
            Function::Sum(v) => v.name(),
            Function::Max(v) => v.name(),
        }
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        match self {
            Function::Constant(v) => v.return_type(input_schema),
            Function::Variable(v) => v.return_type(input_schema),
            Function::Add(v) => v.return_type(input_schema),
            Function::Div(v) => v.return_type(input_schema),
            Function::Mul(v) => v.return_type(input_schema),
            Function::Sub(v) => v.return_type(input_schema),
            Function::Count(v) => v.return_type(),
            Function::Sum(v) => v.return_type(),
            Function::Max(v) => v.return_type(),
        }
    }

    pub fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        match self {
            Function::Constant(v) => v.nullable(input_schema),
            Function::Variable(v) => v.nullable(input_schema),
            Function::Add(v) => v.nullable(input_schema),
            Function::Div(v) => v.nullable(input_schema),
            Function::Mul(v) => v.nullable(input_schema),
            Function::Sub(v) => v.nullable(input_schema),
            Function::Count(v) => v.nullable(input_schema),
            Function::Sum(v) => v.nullable(input_schema),
            Function::Max(v) => v.nullable(input_schema),
        }
    }

    pub fn evaluate(&self, block: &DataBlock) -> Result<DataArrayRef> {
        match self {
            Function::Constant(v) => v.evaluate(block),
            Function::Variable(v) => v.evaluate(block),
            Function::Add(v) => v.evaluate(block),
            Function::Div(v) => v.evaluate(block),
            Function::Mul(v) => v.evaluate(block),
            Function::Sub(v) => v.evaluate(block),
            _ => Err(Error::Unsupported(format!(
                "Unsupported evaluate() for function {}",
                self.name()
            ))),
        }
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        match self {
            Function::Count(ref mut v) => v.accumulate(block),
            Function::Sum(ref mut v) => v.accumulate(block),
            Function::Max(ref mut v) => v.accumulate(block),
            _ => Err(Error::Unsupported(format!(
                "Unsupported accumulate() for function {}",
                self.name()
            ))),
        }
    }

    pub fn aggregate(&self) -> Result<DataArrayRef> {
        match self {
            Function::Count(v) => v.aggregate(),
            Function::Sum(v) => v.aggregate(),
            Function::Max(v) => v.aggregate(),
            _ => Err(Error::Unsupported(format!(
                "Unsupported aggregate() for function {}",
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
            Function::Add(v) => write!(f, "{}", v),
            Function::Div(v) => write!(f, "{}", v),
            Function::Mul(v) => write!(f, "{}", v),
            Function::Sub(v) => write!(f, "{}", v),
            Function::Count(v) => write!(f, "{}", v),
            Function::Sum(v) => write!(f, "{}", v),
            Function::Max(v) => write!(f, "{}", v),
        }
    }
}
