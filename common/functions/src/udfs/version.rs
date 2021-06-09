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
pub struct VersionFunction {
    display_name: String,
}

impl VersionFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(VersionFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl IFunction for VersionFunction {
    fn name(&self) -> &str {
        "VersionFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        Ok(columns[0].clone())
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for VersionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "version")
    }
}
