// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct UdfExampleFunction {
    display_name: String,
}

impl UdfExampleFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(UdfExampleFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl IFunction for UdfExampleFunction {
    fn name(&self) -> &str {
        "UdfExampleFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Constant(
            DataValue::Boolean(Some(true)),
            input_rows,
        ))
    }

    fn num_arguments(&self) -> usize {
        0
    }
}

impl fmt::Display for UdfExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
