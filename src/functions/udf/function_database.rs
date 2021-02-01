// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{udf::IUDFFunction, udf::UDFFunction, Function};

#[derive(Clone, Debug)]
pub struct DatabaseFunction {}

impl DatabaseFunction {
    pub fn try_create(args: &[Function]) -> FuseQueryResult<Function> {
        if !args.is_empty() {
            return Err(FuseQueryError::Internal(
                "database function args length must be zero".to_string(),
            ));
        }

        Ok(Function::UDF(UDFFunction::DatabaseFunction(
            DatabaseFunction {},
        )))
    }
}

impl IUDFFunction for DatabaseFunction {
    fn name(&self) -> &str {
        "database"
    }

    fn return_type(&self, _: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn eval(&mut self, _: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        // currently defaults to `default`, todo: pass ctx to get database
        Ok(DataColumnarValue::Scalar(DataValue::String(Some(
            String::from("default"),
        ))))
    }
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database()")
    }
}
