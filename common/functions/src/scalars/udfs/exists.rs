// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::{DataSchema, DataValue};
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct ExistsFunction;

impl ExistsFunction {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ExistsFunction {}))
    }
}

impl Function for ExistsFunction {
    fn name(&self) -> &str {
        "ExistsFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        match columns[0] {
            DataColumn::Array(_) => {
                Err(ErrorCode::LogicalError(
                    "Logical error: subquery result set must be const."
                ))
            },
            DataColumn::Constant(_, size) => {
                Ok(DataColumn::Constant(DataValue::Boolean(Some(size != 0)), size))
            }
        }
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for ExistsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXISTS")
    }
}
