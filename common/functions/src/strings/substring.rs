// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::ops::Deref;

use common_arrow::arrow::compute;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct SubstringFunction {
    display_name: String,
    /// The expression to substring
    expr: Box<dyn IFunction>,
    /// Substring params
    from: Box<dyn IFunction>,
    len: Box<dyn IFunction>
}

impl SubstringFunction {
    pub fn try_create(
        display_name: &str,
        args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {
        match args.len() {
            3 => Ok(Box::new(SubstringFunction {
                display_name: display_name.to_string(),
                expr: args[0].clone(),
                from: args[1].clone(),
                len: args[2].clone()
            })),
            _ => Result::Err(ErrorCodes::BadArguments(
                "Function Error: Substring function args length must be 3".to_string()
            ))
        }
    }
}

impl IFunction for SubstringFunction {
    fn name(&self) -> &str {
        "substring"
    }

    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        self.expr.return_type(input_schema)
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        let value = self.expr.eval(block)?;
        let from = self.from.eval(block)?;
        let mut from_scalar = 0_i64;
        if let DataColumnarValue::Scalar(from) = from {
            match from {
                DataValue::Int64(Some(from)) => {
                    from_scalar = from - 1;
                }
                DataValue::UInt64(Some(from)) => {
                    from_scalar = (from as i64) - 1;
                }
                _ => {}
            }
        }

        let len = self.len.eval(block)?;
        let mut len_scalar = None;
        if let DataColumnarValue::Scalar(DataValue::UInt64(len)) = len {
            len_scalar = len;
        }

        match value {
            DataColumnarValue::Array(v) => Ok(DataColumnarValue::Array(
                compute::kernels::substring::substring(v.deref(), from_scalar, &len_scalar)
                    .map_err(ErrorCodes::from_arrow)?
            )),
            DataColumnarValue::Scalar(v) => {
                let scalar_array = v.to_array()?;
                let substring_array = compute::kernels::substring::substring(
                    scalar_array.deref(),
                    from_scalar,
                    &len_scalar
                )
                .map_err(ErrorCodes::from_arrow)?;
                let substring_scalar = DataValue::try_from_array(&substring_array, 0)?;
                Ok(DataColumnarValue::Scalar(substring_scalar))
            }
        }
    }
}

impl fmt::Display for SubstringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SUBSTRING({},{},{})", self.expr, self.from, self.len)
    }
}
