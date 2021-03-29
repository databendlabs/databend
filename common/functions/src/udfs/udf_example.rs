// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::{bail, Result};
use common_datablocks::DataBlock;
use common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::IFunction;

#[derive(Clone)]
pub struct UdfExampleFunction;

impl UdfExampleFunction {
    pub fn try_create(_args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(UdfExampleFunction {}))
    }
}

impl IFunction for UdfExampleFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _block: &DataBlock) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(DataValue::Boolean(Some(true))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> Result<()> {
        bail!("Unsupported accumulate for example UDF");
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        bail!("Unsupported accumulate_result for example UDF");
    }

    fn merge(&mut self, _states: &[DataValue]) -> Result<()> {
        bail!("Unsupported merge for example UDF");
    }

    fn merge_result(&self) -> Result<DataValue> {
        bail!("Unsupported merge_result for example UDF");
    }
}

impl fmt::Display for UdfExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "example()")
    }
}
