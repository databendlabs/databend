// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use std::fmt;
use crate::IFunction;

#[derive(Clone)]
pub struct DataBaseFunction;

impl DataBaseFunction {
    pub fn try_create(_args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(DataBaseFunction {}))
    }
}

impl IFunction for DataBaseFunction {
    fn name(&self) -> &str {
        "DataBaseFunction"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _block: &DataBlock) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(DataValue::Utf8(Some("default".to_string()))))
    }
}

impl fmt::Display for DataBaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "database()")
    }
}
