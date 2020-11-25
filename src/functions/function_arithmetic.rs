// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    data_array_arithmetic_op, DataColumnarValue, DataSchema, DataType, DataValueArithmeticOperator,
};
use crate::error::FuseQueryResult;
use crate::functions::Function;

#[derive(Clone)]
pub struct ArithmeticFunction {
    op: DataValueArithmeticOperator,
    left: Box<Function>,
    right: Box<Function>,
}

impl ArithmeticFunction {
    pub fn create(op: DataValueArithmeticOperator, args: &[Function]) -> FuseQueryResult<Function> {
        Ok(Function::Arithmetic(ArithmeticFunction {
            op,
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
        }))
    }

    pub fn name(&self) -> &'static str {
        match self.op {
            DataValueArithmeticOperator::Add => "AddFunction",
            DataValueArithmeticOperator::Sub => "SubFunction",
            DataValueArithmeticOperator::Mul => "MulFunction",
            DataValueArithmeticOperator::Div => "DivFunction",
        }
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.left.return_type(input_schema)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn evaluate(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(data_array_arithmetic_op(
            self.op.clone(),
            &self.left.evaluate(block)?,
            &self.right.evaluate(block)?,
        )?))
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} {} {:?}", self.left, self.op, self.right)
    }
}
