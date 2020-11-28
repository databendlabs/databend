// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{
    data_array_comparison_op, DataColumnarValue, DataSchema, DataType, DataValueComparisonOperator,
};
use crate::error::{FuseQueryError, FuseQueryResult};

use crate::functions::Function;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator,
    left: Box<Function>,
    right: Box<Function>,
    saved: Option<DataColumnarValue>,
}

impl ComparisonFunction {
    pub fn try_create(
        op: DataValueComparisonOperator,
        args: &[Function],
    ) -> FuseQueryResult<Function> {
        Ok(Function::Comparison(ComparisonFunction {
            op,
            left: Box::from(args[0].clone()),
            right: Box::from(args[1].clone()),
            saved: None,
        }))
    }

    pub fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Boolean)
    }

    pub fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    pub fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.left.eval(block)?;
        self.right.eval(block)?;
        self.saved = Some(DataColumnarValue::Array(data_array_comparison_op(
            self.op.clone(),
            &self.left.result()?,
            &self.right.result()?,
        )?));
        Ok(())
    }

    pub fn result(&self) -> FuseQueryResult<DataColumnarValue> {
        self.saved
            .clone()
            .ok_or_else(|| FuseQueryError::Internal("Saved cannot none".to_string()))
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} {} {:?}", self.left, self.op, self.right)
    }
}
