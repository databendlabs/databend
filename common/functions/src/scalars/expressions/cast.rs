// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct CastFunction {
    display_name: String,
    /// The data type to cast to
    cast_type: DataType,
}

impl CastFunction {
    pub fn create(display_name: String, cast_type: DataType) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name,
            cast_type,
        }))
    }
}

impl Function for CastFunction {
    fn name(&self) -> &str {
        "CastFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    // TODO
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        let series = columns[0].to_minimal_array()?;
        let column: DataColumn = series.cast_with_type(&self.cast_type)?.into();
        Ok(column.resize_constant(input_rows))
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST")
    }
}
