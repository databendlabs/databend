// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use log::debug;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::udf::DatabaseFunction;
use crate::functions::udf::ToTypeNameFunction;
use crate::functions::udf::UDFExampleFunction;
use crate::functions::FactoryFuncRef;

pub trait IUDFFunction {
    fn name(&self) -> &str;
    fn return_type(&self, _: &DataSchema) -> FuseQueryResult<DataType>;
    fn eval(&mut self, _: &DataBlock) -> FuseQueryResult<DataColumnarValue>;

    fn set_depth(&mut self, _: usize) {}

    fn nullable(&self, _: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn accumulate(&mut self, _: &DataBlock) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported accumulate for {} UDF",
            self.name()
        )))
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported accumulate_result for {} UDF",
            self.name()
        )))
    }

    fn merge(&mut self, _: &[DataValue]) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported merge for {} UDF",
            self.name()
        )))
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Err(FuseQueryError::Internal(format!(
            "Unsupported merge_result for {} UDF",
            self.name()
        )))
    }
}

// #[derive(Clone, Debug)]
// pub enum UDFFunction {
//     UDFExampleFunction(UDFExampleFunction),
//     ...
// }

apply_macros! {
    ("example", UDFExampleFunction),
    ("totypename", ToTypeNameFunction),
    ("database", DatabaseFunction)
}
