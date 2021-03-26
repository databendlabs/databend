// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use crate::common_datablocks::DataBlock;
use crate::common_datavalues::{
    self as datavalues, DataColumnarValue, DataSchema, DataType, DataValue, DataValueLogicOperator,
};
use crate::logics::{LogicAndFunction, LogicOrFunction};
use crate::{FactoryFuncRef, FunctionError, FunctionResult, IFunction};

#[derive(Clone)]
pub struct LogicFunction {
    depth: usize,
    op: DataValueLogicOperator,
    left: Box<dyn IFunction>,
    right: Box<dyn IFunction>,
    saved: Option<DataColumnarValue>,
}

impl LogicFunction {
    pub fn register(map: FactoryFuncRef) -> FunctionResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("and", LogicAndFunction::try_create_func);
        map.insert("or", LogicOrFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(
        op: DataValueLogicOperator,
        args: &[Box<dyn IFunction>],
    ) -> FunctionResult<Box<dyn IFunction>> {
        if args.len() != 2 {
            return Err(FunctionError::build_internal_error(format!(
                "Logic function {} args length must be 2",
                op
            )));
        }

        Ok(Box::new(LogicFunction {
            depth: 0,
            op,
            left: args[0].clone(),
            right: args[1].clone(),
            saved: None,
        }))
    }
}

impl IFunction for LogicFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FunctionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FunctionResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FunctionResult<DataColumnarValue> {
        Ok(DataColumnarValue::Array(datavalues::data_array_logic_op(
            self.op.clone(),
            &self.left.eval(block)?,
            &self.right.eval(block)?,
        )?))
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

    fn is_aggregator(&self) -> bool {
        self.left.is_aggregator() || self.right.is_aggregator()
    }
}

impl fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
