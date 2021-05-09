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

pub trait IFunction: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;
    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType>;
    fn nullable(&self, input_schema: &DataSchema) -> Result<bool>;
    fn set_depth(&mut self, _depth: usize) {}

    // eval is only for none aggregate function
    fn eval(&self, _block: &DataBlock) -> Result<DataColumnarValue> {
        Result::Err(ErrorCodes::UnImplement(format!(
            "Function Error: '{}' eval unimplemented",
            self.name()
        )))
    }

    fn accumulate(&mut self, _block: &DataBlock) -> Result<()> {
        Result::Err(ErrorCodes::UnImplement(format!(
            "Function Error: '{}' accumulate unimplemented",
            self.name()
        )))
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Result::Err(ErrorCodes::UnImplement(format!(
            "Function Error: '{}' accumulate_result unimplemented",
            self.name()
        )))
    }

    fn merge(&mut self, _states: &[DataValue]) -> Result<()> {
        Result::Err(ErrorCodes::UnImplement(format!(
            "Function Error: '{}' merge unimplemented",
            self.name()
        )))
    }

    fn merge_result(&self) -> Result<DataValue> {
        Result::Err(ErrorCodes::UnImplement(format!(
            "Function Error: '{}' merge_result unimplemented",
            self.name()
        )))
    }

    fn is_aggregator(&self) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(IFunction);
