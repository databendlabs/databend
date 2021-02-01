// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::datablocks::DataBlock;
use crate::datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{udf::IUDFFunction, udf::UDFFunction, Function};

#[derive(Clone, Debug)]
pub struct ToTypeNameFunction {
    arg: Box<Function>,
}

impl ToTypeNameFunction {
    pub fn try_create(args: &[Function]) -> FuseQueryResult<Function> {
        if args.len() != 1 {
            return Err(FuseQueryError::Internal(
                "toTypeName function args length must be one".to_string(),
            ));
        }

        Ok(Function::UDF(UDFFunction::ToTypeNameFunction(
            ToTypeNameFunction {
                arg: Box::from(args[0].clone()),
            },
        )))
    }
}

impl IUDFFunction for ToTypeNameFunction {
    fn name(&self) -> &str {
        "toTypeName"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn eval(&mut self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        let arg_type = format!("{}", self.arg.return_type(block.schema())?);
        Ok(DataColumnarValue::Scalar(DataValue::String(Some(arg_type))))
    }
}

impl fmt::Display for ToTypeNameFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "toTypeName({:?})", self.arg)
    }
}
