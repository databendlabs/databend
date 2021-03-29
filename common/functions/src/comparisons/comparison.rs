// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::{bail, ensure, Result};
use common_datablocks::DataBlock;
use common_datavalues::{
    self as datavalues, DataColumnarValue, DataSchema, DataType, DataValue,
    DataValueComparisonOperator,
};

use crate::comparisons::{
    ComparisonEqFunction, ComparisonGtEqFunction, ComparisonGtFunction, ComparisonLtEqFunction,
    ComparisonLtFunction, ComparisonNotEqFunction,
};
use crate::{FactoryFuncRef, IFunction};

#[derive(Clone)]
pub struct ComparisonFunction {
    depth: usize,
    op: DataValueComparisonOperator,
    left: Box<dyn IFunction>,
    right: Box<dyn IFunction>,
    saved: Option<DataColumnarValue>,
}

impl ComparisonFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.as_ref().lock();
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
    ) -> Result<Box<dyn IFunction>> {
        ensure!(
            args.len() == 2,
            "Function Error: Comparison function {} args length must be 2",
            op
        );

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
    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
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

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        self.left.accumulate(block)?;
        self.right.accumulate(block)
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        bail!(
            "Unsupported accumulate_result operation for function {}",
            self.op
        );
    }

    fn merge(&mut self, _states: &[DataValue]) -> Result<()> {
        bail!("Unsupported merge operation for function {}", self.op);
    }

    fn merge_result(&self) -> Result<DataValue> {
        bail!(
            "Unsupported merge_result operation for function {}",
            self.op
        );
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
