// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    data_array_comparison_op, DataColumnarValue, DataSchema, DataType, DataValueComparisonOperator,
};
use crate::error::FuseQueryResult;

use crate::functions::Function;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator,
    left: Box<Function>,
    right: Box<Function>,
}

impl ComparisonFunction {
    pub fn create(op: DataValueComparisonOperator, args: &[Function]) -> FuseQueryResult<Function> {
        Ok(Function::Comparison(ComparisonFunction {
            op,
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
        }))
    }

    pub fn name(&self) -> &'static str {
        match self.op {
            DataValueComparisonOperator::Eq => "EqualFunction",
            DataValueComparisonOperator::Lt => "LessThanFunction",
            DataValueComparisonOperator::LtEq => "LessOrEqualFunction",
            DataValueComparisonOperator::Gt => "GreaterThanFunction",
            DataValueComparisonOperator::GtEq => "GreaterOrEqualFunction",
        }
    }

    pub fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Boolean)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn evaluate(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(data_array_comparison_op(
            self.op.clone(),
            &self.left.evaluate(block)?,
            &self.right.evaluate(block)?,
        )?))
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} {} {:?}", self.left, self.op, self.right)
    }
}
