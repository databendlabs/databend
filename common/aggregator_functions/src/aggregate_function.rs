// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use dyn_clone::DynClone;

pub trait IAggreagteFunction: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;
    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, input_schema: &DataSchema) -> Result<bool>;
    fn set_depth(&mut self, _depth: usize) {}
    fn accumulate(&mut self, _block: &DataBlock) -> Result<()>;
    fn accumulate_result(&self) -> Result<Vec<DataValue>>;
    fn merge(&mut self, _states: &[DataValue]) -> Result<()>;
    fn merge_result(&self) -> Result<DataValue>;
}

dyn_clone::clone_trait_object!(IAggreagteFunction);
