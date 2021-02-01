// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::{udf::IUDFFunction, udf::UDFFunction, Function};

#[derive(Clone, Debug)]
pub struct UDFExampleFunction {}

impl UDFExampleFunction {
    pub fn try_create(_args: &[Function]) -> FuseQueryResult<Function> {
        Ok(Function::UDF(UDFFunction::UDFExampleFunction(
            UDFExampleFunction {},
        )))
    }
}

impl IUDFFunction for UDFExampleFunction {
    fn name(&self) -> &str {
        "example"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn eval(&mut self, _block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(DataValue::Boolean(Some(true))))
    }
}

impl fmt::Display for UDFExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "example()")
    }
}
