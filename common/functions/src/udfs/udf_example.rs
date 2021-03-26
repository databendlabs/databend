// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::{FunctionError, FunctionResult, IFunction};

#[derive(Clone)]
pub struct UdfExampleFunction;

impl UdfExampleFunction {
    pub fn try_create(_args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        Ok(Box::new(UdfExampleFunction {}))
    }
}

impl IFunction for UdfExampleFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FunctionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FunctionResult<bool> {
        Ok(false)
    }

    fn eval(&self, _block: &DataBlock) -> FunctionResult<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(DataValue::Boolean(Some(true))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FunctionResult<()> {
        Err(FunctionError::build_internal_error(
            "Unsupported accumulate for example UDF".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>> {
        Err(FunctionError::build_internal_error(
            "Unsupported accumulate_result for example UDF".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FunctionResult<()> {
        Err(FunctionError::build_internal_error(
            "Unsupported merge for example UDF".to_string(),
        ))
    }

    fn merge_result(&self) -> FunctionResult<DataValue> {
        Err(FunctionError::build_internal_error(
            "Unsupported merge_result for example UDF".to_string(),
        ))
    }
}

impl fmt::Display for UdfExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "example()")
    }
}
