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
pub struct ToTypeNameFunction {
    display_name: String
}

impl ToTypeNameFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ToTypeNameFunction {
            display_name: display_name.to_string()
        }))
    }
}

impl IFunction for ToTypeNameFunction {
    fn name(&self) -> &str {
        "ToTypeNameFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue> {
        let type_name = format!("{}", columns[0].data_type());
        Ok(DataColumnarValue::Constant(
            DataValue::Utf8(Some(type_name)),
            input_rows
        ))
    }
}

impl fmt::Display for ToTypeNameFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ToTypeName")
    }
}
