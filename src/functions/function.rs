// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub enum Function {
    Constant(ConstantFunction),
    Variable(VariableFunction),
    Add(arithmetic::AddFunction),
    Sub(arithmetic::SubFunction),
    Div(arithmetic::DivFunction),
    Mul(arithmetic::MulFunction),
    Count(aggregate::CountAggregateFunction),
}

impl Function {
    pub fn name(&self) -> String {
        match self {
            Function::Constant(v) => v.name(),
            Function::Variable(v) => v.name(),
            Function::Add(v) => v.name(),
            Function::Div(v) => v.name(),
            Function::Mul(v) => v.name(),
            Function::Sub(v) => v.name(),
            Function::Count(v) => v.name(),
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
            Function::Count(v) => v.return_type(input_schema),
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
            _ => Err(Error::Unsupported(format!("{}.evaluate()", self.name()))),
        }
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        match self {
            Function::Count(ref mut v) => v.accumulate(block),
            _ => Err(Error::Unsupported(format!("{}.accumulate()", self.name()))),
        }
    }

    pub fn aggregate(&self) -> Result<DataArrayRef> {
        match self {
            Function::Count(v) => v.aggregate(),
            _ => Err(Error::Unsupported(format!("{}.aggregate()", self.name()))),
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
        }
    }
}
