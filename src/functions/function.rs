// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use dyn_clone::DynClone;
use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;

pub trait IFunction: fmt::Display + Sync + Send + DynClone {
    fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType>;
    fn nullable(&self, input_schema: &DataSchema) -> FuseQueryResult<bool>;
    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue>;
    fn set_depth(&mut self, depth: usize);
    fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()>;
    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>>;
    fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()>;
    fn merge_result(&self) -> FuseQueryResult<DataValue>;
    fn is_aggregator(&self) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(IFunction);
