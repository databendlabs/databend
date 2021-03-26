// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use crate::common_datablocks::DataBlock;
use crate::common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::{FunctionResult, IFunction};

#[derive(Clone, Debug)]
pub struct LiteralFunction {
    value: DataValue,
}

impl LiteralFunction {
    pub fn try_create(value: DataValue) -> FunctionResult<Box<dyn IFunction>> {
        Ok(Box::new(LiteralFunction { value }))
    }
}

impl IFunction for LiteralFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FunctionResult<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FunctionResult<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _block: &DataBlock) -> FunctionResult<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(self.value.clone()))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FunctionResult<()> {
        Ok(())
    }

    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>> {
        Ok(vec![self.value.clone()])
    }

    fn merge(&mut self, _states: &[DataValue]) -> FunctionResult<()> {
        Ok(())
    }

    fn merge_result(&self) -> FunctionResult<DataValue> {
        Ok(self.value.clone())
    }
}

impl fmt::Display for LiteralFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
