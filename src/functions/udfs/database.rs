// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;

#[derive(Clone)]
pub struct DatabaseFunction {}

impl DatabaseFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> FuseQueryResult<Box<dyn IFunction>> {
        if !args.is_empty() {
            return Err(FuseQueryError::Internal(
                "ToTypeName function args length must be zero".to_string(),
            ));
        }

        Ok(Box::new(DatabaseFunction {}))
    }
}

impl IFunction for DatabaseFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn eval(&self, _: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        // TODO, pass context
        Ok(DataColumnarValue::Scalar(DataValue::String(Some(
            "default".to_string(),
        ))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Unsupported accumulate for database Function".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::Internal(
            "Unsupported accumulate_result for database Function".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Unsupported merge for database Function".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(
            "Unsupported merge_result for database Function".to_string(),
        ))
    }
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database()")
    }
}
