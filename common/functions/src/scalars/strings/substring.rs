// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct SubstringFunction {
    display_name: String,
}

impl SubstringFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SubstringFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for SubstringFunction {
    fn name(&self) -> &str {
        "substring"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        // TODO: make this function support column value as arguments rather than literal
        let mut from = match columns[1].data_type() {
            DataType::UInt64 => Ok(columns[1]
                .to_minimal_array()?
                .u64()?
                .downcast_ref()
                .value(0) as i64),
            DataType::Int64 => Ok(columns[1]
                .to_minimal_array()?
                .i64()?
                .downcast_ref()
                .value(0) as i64),
            other => Err(ErrorCode::BadArguments(format!(
                "Unsupport datatype {:?} as argument",
                other
            ))),
        }?;

        if from >= 1 {
            from -= 1;
        }

        let mut end = None;
        if columns.len() >= 3 {
            match columns[2].data_type() {
                DataType::UInt64 => {
                    end = Some(
                        columns[2]
                            .to_minimal_array()?
                            .u64()?
                            .downcast_ref()
                            .value(0),
                    );
                }

                DataType::Int64 => {
                    end = Some(
                        columns[2]
                            .to_minimal_array()?
                            .i16()?
                            .downcast_ref()
                            .value(0) as u64,
                    );
                }

                other => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Unsupport datatype {:?} as argument",
                        other
                    )))
                }
            }
        }

        // todo, move these to datavalues
        let value = columns[0].to_array()?;
        let arrow_array = value.get_array_ref();
        let result =
            compute::kernels::substring::substring(arrow_array.as_ref(), from, &end)? as ArrayRef;
        Ok(result.into())
    }

    // substring(str, from)
    // substring(str, from, end)
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((2, 3))
    }
}

impl fmt::Display for SubstringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SUBSTRING")
    }
}
