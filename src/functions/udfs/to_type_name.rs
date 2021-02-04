// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::contexts::FuseQueryContextRef;
use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;

#[derive(Clone)]
pub struct ToTypeNameFunction {
    arg: Box<dyn IFunction>,
}

impl ToTypeNameFunction {
    pub fn try_create(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        if args.len() != 1 {
            return Err(FuseQueryError::Internal(
                "The argument size of function database must be one".to_string(),
            ));
        }

        Ok(Box::new(ToTypeNameFunction {
            arg: args[0].clone(),
        }))
    }
}

impl IFunction for ToTypeNameFunction {
    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        let type_name = format!("{}", self.arg.return_type(block.schema())?);
        Ok(DataColumnarValue::Scalar(DataValue::String(Some(
            type_name,
        ))))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, _block: &DataBlock) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Unsupported accumulate for toTypeName Function".to_string(),
        ))
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::Internal(
            "Unsupported accumulate_result for toTypeName Function".to_string(),
        ))
    }

    fn merge(&mut self, _states: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Unsupported merge for toTypeName Function".to_string(),
        ))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(
            "Unsupported merge_result for toTypeName Function".to_string(),
        ))
    }
}

impl fmt::Display for ToTypeNameFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "toTypeName({})", self.arg)
    }
}
