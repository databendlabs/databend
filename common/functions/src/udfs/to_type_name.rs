// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::Function;

#[derive(Clone)]
pub struct ToTypeNameFunction {
    display_name: String,
}

impl ToTypeNameFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ToTypeNameFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for ToTypeNameFunction {
    fn name(&self) -> &str {
        "ToTypeNameFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        let type_name = format!("{}", columns[0].data_type());
        Ok(DataColumn::Constant(
            DataValue::Utf8(Some(type_name)),
            input_rows,
        ))
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for ToTypeNameFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "toTypeName")
    }
}
