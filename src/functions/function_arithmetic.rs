// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues;
use crate::datavalues::{
    DataColumnarValue, DataSchema, DataType, DataValue, DataValueArithmeticOperator,
};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::Function;

#[derive(Clone)]
pub struct ArithmeticFunction {
    depth: usize,
    op: DataValueArithmeticOperator,
    left: Box<Function>,
    right: Box<Function>,
}

impl ArithmeticFunction {
    pub fn try_create_add_func(args: &[Function]) -> FuseQueryResult<Function> {
        Self::try_create(DataValueArithmeticOperator::Add, args)
    }

    pub fn try_create_sub_func(args: &[Function]) -> FuseQueryResult<Function> {
        Self::try_create(DataValueArithmeticOperator::Sub, args)
    }

    pub fn try_create_mul_func(args: &[Function]) -> FuseQueryResult<Function> {
        Self::try_create(DataValueArithmeticOperator::Mul, args)
    }

    pub fn try_create_div_func(args: &[Function]) -> FuseQueryResult<Function> {
        Self::try_create(DataValueArithmeticOperator::Div, args)
    }

    fn try_create(op: DataValueArithmeticOperator, args: &[Function]) -> FuseQueryResult<Function> {
        if args.len() != 2 {
            return Err(FuseQueryError::Internal(
                "Arithmetic function args length must be 2".to_string(),
            ));
        }

        Ok(Function::Arithmetic(ArithmeticFunction {
            depth: 0,
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

    pub fn set_depth(&mut self, depth: usize) {
        self.left.set_depth(depth);
        self.right.set_depth(depth + 1);
        self.depth = depth;
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(
            datavalues::data_array_arithmetic_op(
                self.op.clone(),
                &self.left.eval(block)?,
                &self.right.eval(block)?,
            )?,
        ))
    }

    pub fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.left.accumulate(&block)?;
        self.right.accumulate(&block)
    }

    pub fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok([
            &self.left.accumulate_result()?[..],
            &self.right.accumulate_result()?[..],
        ]
        .concat())
    }

    pub fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        self.left.merge(states)?;
        self.right.merge(states)
    }

    pub fn merge_result(&self) -> FuseQueryResult<DataValue> {
        datavalues::data_value_arithmetic_op(
            self.op.clone(),
            self.left.merge_result()?,
            self.right.merge_result()?,
        )
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({:?} {} {:?})", self.left, self.op, self.right)
    }
}
