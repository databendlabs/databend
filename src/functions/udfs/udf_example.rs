// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
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
        Err(FuseQueryError::Internal(
            "Unsupported accumulate for example UDF".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::Internal(
            "Unsupported accumulate_result for example UDF".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Unsupported merge for example UDF".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(
            "Unsupported merge_result for example UDF".to_string(),
        ))
    }
}

impl fmt::Display for UDFExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "example()")
    }
}
