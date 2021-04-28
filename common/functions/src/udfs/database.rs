// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_exception::{Result, ErrorCodes};
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;

use crate::IFunction;

#[derive(Clone)]
pub struct DatabaseFunction {
    arg: Box<dyn IFunction>
}

impl DatabaseFunction {
    pub fn try_create(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        match args.len() {
            1 => Ok(Box::new(Self { arg: args[0].clone() })),
            _ => Result::Err(ErrorCodes::BadArguments("The argument size of function database must be one".to_string()))
        }
    }
}

impl IFunction for DatabaseFunction {
    fn name(&self) -> &str {
        "DatabaseFunction"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        self.arg.eval(block)
    }
}

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database()")
    }
}
