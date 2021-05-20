// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct AliasFunction {
    alias: String
}

impl AliasFunction {
    pub fn try_create(alias: String) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(AliasFunction { alias }))
    }
}

impl IFunction for AliasFunction {
    fn name(&self) -> &str {
        "AliasFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        Ok(columns[0].clone())
    }
}

impl fmt::Display for AliasFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.alias)
    }
}
