// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct SipHashFunction {
    display_name: String,
}

impl SipHashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SipHashFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for SipHashFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Utf8
            | DataType::Binary => Ok(DataType::UInt64),
            _ => Result::Err(ErrorCode::BadArguments(format!(
                "Function Error: {} does not support {} type parameters",
                self.display_name, args[0]
            ))),
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        let series = columns[0].to_minimal_array()?;
        let hasher = DFHasher::SipHasher(DefaultHasher::new());
        let res: DataColumn = series.vec_hash(hasher)?.into();
        Ok(res.resize_constant(input_rows))
    }
}

impl fmt::Display for SipHashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
