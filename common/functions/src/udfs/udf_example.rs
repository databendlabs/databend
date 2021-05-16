// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct UdfExampleFunction {
    display_name: String
}

impl UdfExampleFunction {
    pub fn try_create(
        display_name: &str,
        _args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(UdfExampleFunction {
            display_name: display_name.to_string()
        }))
    }
}

impl IFunction for UdfExampleFunction {
    fn name(&self) -> &str {
        "UdfExampleFunction"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _block: &DataBlock) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Constant(DataValue::Boolean(Some(true))))
    }
}

impl fmt::Display for UdfExampleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
