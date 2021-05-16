// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone, Debug)]
pub struct ColumnFunction {
    value: String,
    saved: Option<DataValue>
}

impl ColumnFunction {
    pub fn try_create(value: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ColumnFunction {
            value: value.to_string(),
            saved: None
        }))
    }
}

impl IFunction for ColumnFunction {
    fn name(&self) -> &str {
        "ColumnFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        let field = if self.value == "*" {
            input_schema.field(0)
        } else {
            input_schema.field_with_name(self.value.as_str())?
        };
        Ok(field.is_nullable())
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        Ok(columns[0].clone())
    }
}

impl fmt::Display for ColumnFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
