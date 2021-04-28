// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::ensure;
use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;

use crate::IFunction;

#[derive(Clone)]
pub struct DatabaseFunction {
    display_name: String,
    arg: Box<dyn IFunction>
}

impl DatabaseFunction {
    pub fn try_create(
        display_name: &str,
        args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {
        ensure!(
            args.len() == 1,
            "The argument size of function {} must be one",
            display_name
        );

        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            arg: args[0].clone()
        }))
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
        write!(f, "{}()", self.display_name)
    }
}
