// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues;
use crate::datavalues::{
    DataColumnarValue, DataSchema, DataType, DataValue, DataValueArithmeticOperator,
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
    pub fn try_create(
        op: DataValueArithmeticOperator,
        args: &[Function],
    ) -> FuseQueryResult<Function> {
        Ok(Function::Arithmetic(ArithmeticFunction {
            op,
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
        }))
    }

    pub fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        datavalues::numerical_coercion(
            format!("{}", self.op).as_str(),
            &self.left.return_type(input_schema)?,
            &self.right.return_type(input_schema)?,
        )
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.left.eval(block)?;
        self.right.eval(block)?;

        Ok(())
    }

    pub fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        self.left.merge(states)?;
        self.right.merge(states)?;

        Ok(())
    }

    pub fn state(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok(vec![])
    }

    pub fn result(&self) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(
            datavalues::data_array_arithmetic_op(
                self.op.clone(),
                &self.left.result()?,
                &self.right.result()?,
            )?,
        ))
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} {} {:?}", self.left, self.op, self.right)
    }
}
