// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_arrow::arrow::array::Int64Array;
use common_arrow::arrow::compute;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::UInt64Array;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct SubstringFunction {
    display_name: String
}

impl SubstringFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(SubstringFunction {
            display_name: display_name.to_string()
        }))
    }
}

impl IFunction for SubstringFunction {
    fn name(&self) -> &str {
        "substring"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        // TODO: make this function support column value as arguments rather than literal
        let from = columns[1]
            .to_array()?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        let end = {
            if columns.len() >= 3 {
                let array = columns[2].to_array()?;
                let v = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Some(v.value(0))
            } else {
                None
            }
        };

        let value = columns[0].to_array()?;
        Ok(DataColumnarValue::Array(
            compute::kernels::substring::substring(value.as_ref(), from, &end)?
        ))
    }
}

impl fmt::Display for SubstringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SUBSTRING")
    }
}
