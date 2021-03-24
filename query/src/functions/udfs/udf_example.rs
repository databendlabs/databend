// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use fuse_query_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::datablocks::DataBlock;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;
use crate::sessions::FuseQueryContextRef;

#[derive(Clone)]
pub struct UDFExampleFunction;

impl UDFExampleFunction {
    pub fn try_create(
        _ctx: FuseQueryContextRef,
        _args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        Ok(Box::new(UDFExampleFunction {}))
    }
}

impl IFunction for UDFExampleFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn eval(&self, _block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(DataValue::Boolean(Some(true))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported accumulate for example UDF".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported accumulate_result for example UDF".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported merge for example UDF".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported merge_result for example UDF".to_string(),
        ))
    }
}

impl fmt::Display for UDFExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "example()")
    }
}
