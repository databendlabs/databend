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

#[derive(Clone, Debug)]
pub struct LiteralFunction {
    value: DataValue
}

impl LiteralFunction {
    pub fn try_create(value: DataValue) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(LiteralFunction { value }))
    }
}

impl IFunction for LiteralFunction {
    fn name(&self) -> &str {
        "LiteralFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Constant(self.value.clone(), input_rows))
    }
}

impl fmt::Display for LiteralFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
