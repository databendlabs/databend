// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues;
use crate::datavalues::{
    DataColumnarValue, DataSchema, DataType, DataValue, DataValueComparisonOperator,
};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::comparisons::{
    ComparisonEqFunction, ComparisonGtEqFunction, ComparisonGtFunction, ComparisonLtEqFunction,
    ComparisonLtFunction, ComparisonNotEqFunction,
};
use crate::functions::{FactoryFuncRef, IFunction};

#[derive(Clone)]
pub struct ComparisonFunction {
    depth: usize,
    op: DataValueComparisonOperator,
    left: Box<dyn IFunction>,
    right: Box<dyn IFunction>,
    saved: Option<DataColumnarValue>,
}

impl ComparisonFunction {
    pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("=", ComparisonEqFunction::try_create_func);
        map.insert("<", ComparisonLtFunction::try_create_func);
        map.insert(">", ComparisonGtFunction::try_create_func);
        map.insert("<=", ComparisonLtEqFunction::try_create_func);
        map.insert(">=", ComparisonGtEqFunction::try_create_func);
        map.insert("!=", ComparisonNotEqFunction::try_create_func);
        map.insert("<>", ComparisonNotEqFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(
        op: DataValueComparisonOperator,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        if args.len() != 2 {
            return Err(FuseQueryError::Internal(format!(
                "Comparison function {} args length must be 2",
                op
            )));
        }

        Ok(Box::new(ComparisonFunction {
            depth: 0,
            op,
            left: args[0].clone(),
            right: args[1].clone(),
            saved: None,
        }))
    }
}

impl IFunction for ComparisonFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(
            datavalues::data_array_comparison_op(
                self.op.clone(),
                &self.left.eval(block)?,
                &self.right.eval(block)?,
            )?,
        ))
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        self.left.accumulate(block)?;
        self.right.accumulate(block)
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported accumulate_result operation for function {}",
            self.op
        )))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported merge operation for function {}",
            self.op
        )))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported merge_result operation for function {}",
            self.op
        )))
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
