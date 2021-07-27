// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct LiteralFunction {
    value: DataValue,
}

impl LiteralFunction {
    pub fn try_create(value: DataValue) -> Result<Box<dyn Function>> {
        Ok(Box::new(LiteralFunction { value }))
    }
}

impl Function for LiteralFunction {
    fn name(&self) -> &str {
        "LiteralFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        Ok(DataColumn::Constant(self.value.clone(), input_rows))
    }

    fn num_arguments(&self) -> usize {
        0
    }
}

impl fmt::Display for LiteralFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
