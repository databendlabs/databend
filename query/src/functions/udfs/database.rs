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
pub struct DatabaseFunction {
    ctx: FuseQueryContextRef,
}

impl DatabaseFunction {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        if !args.is_empty() {
            return Err(FuseQueryError::build_internal_error(
                "The argument size of function database must be zero".to_string(),
            ));
        }

        Ok(Box::new(DatabaseFunction { ctx }))
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
        Ok(DataColumnarValue::Scalar(DataValue::String(Some(
            self.ctx.get_default_db()?,
        ))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported accumulate for database Function".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported accumulate_result for database Function".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported merge for database Function".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::build_internal_error(
            "Unsupported merge_result for database Function".to_string(),
        ))
    }
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database()")
    }
}
