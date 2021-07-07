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

#[derive(Clone, Debug)]
pub struct InListFunction {
    list: Vec<DataValue>,
    negated: bool,
}

impl InListFunction {
    pub fn try_create(list: Vec<DataValue>, negated: bool) -> Result<Box<dyn Function>> {
        Ok(Box::new(InListFunction { list, negated }))
    }
}

impl Function for InListFunction {
    fn name(&self) -> &str {
        "InListFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.list[0].is_null())
    }

    fn eval(&self, _columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        Ok(DataColumn::Constant(self.list[0].clone(), input_rows))
    }

    fn num_arguments(&self) -> usize {
        0
    }
}

impl fmt::Display for InListFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self.negated {
            true => "NoT",
            false => "",
        };
        write!(f, "{} IN {:?}", s, self.list)
    }
}
