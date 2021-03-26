// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use dyn_clone::DynClone;

use crate::common_datablocks::DataBlock;
use crate::common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::FunctionResult;

pub trait IFunction: fmt::Display + Sync + Send + DynClone {
    fn return_type(&self, input_schema: &DataSchema) -> FunctionResult<DataType>;
    fn nullable(&self, input_schema: &DataSchema) -> FunctionResult<bool>;
    fn eval(&self, block: &DataBlock) -> FunctionResult<DataColumnarValue>;
    fn set_depth(&mut self, depth: usize);
    fn accumulate(&mut self, block: &DataBlock) -> FunctionResult<()>;
    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>>;
    fn merge(&mut self, states: &[DataValue]) -> FunctionResult<()>;
    fn merge_result(&self) -> FunctionResult<DataValue>;
    fn is_aggregator(&self) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(IFunction);
