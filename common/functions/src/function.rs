// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use dyn_clone::DynClone;

pub trait IFunction: fmt::Display + Sync + Send + DynClone {
    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType>;
    fn nullable(&self, input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue>;
    fn set_depth(&mut self, depth: usize);
    fn accumulate(&mut self, block: &DataBlock) -> Result<()>;
    fn accumulate_result(&self) -> Result<Vec<DataValue>>;
    fn merge(&mut self, states: &[DataValue]) -> Result<()>;
    fn merge_result(&self) -> Result<DataValue>;
    fn is_aggregator(&self) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(IFunction);
