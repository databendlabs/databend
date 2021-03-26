// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use crate::common_datablocks::DataBlock;
use crate::common_datavalues::{
    self as datavalues, DataColumnarValue, DataSchema, DataType, DataValue,
    DataValueComparisonOperator,
};
use crate::comparisons::{
    ComparisonEqFunction, ComparisonGtEqFunction, ComparisonGtFunction, ComparisonLtEqFunction,
    ComparisonLtFunction, ComparisonNotEqFunction,
};
use crate::{FactoryFuncRef, FunctionError, FunctionResult, IFunction};

#[derive(Clone)]
pub struct ComparisonFunction {
    depth: usize,
    op: DataValueComparisonOperator,
    left: Box<dyn IFunction>,
    right: Box<dyn IFunction>,
    saved: Option<DataColumnarValue>,
}

impl ComparisonFunction {
    pub fn register(map: FactoryFuncRef) -> FunctionResult<()> {
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
    ) -> FunctionResult<Box<dyn IFunction>> {
        if args.len() != 2 {
            return Err(FunctionError::build_internal_error(format!(
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
    fn return_type(&self, _input_schema: &DataSchema) -> FunctionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FunctionResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FunctionResult<DataColumnarValue> {
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

    fn accumulate(&mut self, block: &DataBlock) -> FunctionResult<()> {
        self.left.accumulate(block)?;
        self.right.accumulate(block)
    }

    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>> {
        Err(FunctionError::build_internal_error(format!(
            "Unsupported accumulate_result operation for function {}",
            self.op
        )))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FunctionResult<()> {
        Err(FunctionError::build_internal_error(format!(
            "Unsupported merge operation for function {}",
            self.op
        )))
    }

    fn merge_result(&self) -> FunctionResult<DataValue> {
        Err(FunctionError::build_internal_error(format!(
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
