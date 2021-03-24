// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use fuse_query_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::datablocks::DataBlock;
use crate::error::FuseQueryResult;
use crate::functions::IFunction;

#[derive(Clone, Debug)]
pub struct LiteralFunction {
    value: DataValue,
}

impl LiteralFunction {
    pub fn try_create(value: DataValue) -> FuseQueryResult<Box<dyn IFunction>> {
        Ok(Box::new(LiteralFunction { value }))
    }
}

impl IFunction for LiteralFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(self.value.clone()))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FuseQueryResult<()> {
        Ok(())
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok(vec![self.value.clone()])
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Ok(())
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Ok(self.value.clone())
    }
}

impl fmt::Display for LiteralFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
