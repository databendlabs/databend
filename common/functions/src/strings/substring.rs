// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow::array::Int64Array;
use common_arrow::arrow::compute;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::UInt64Array;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::FunctionCtx;
use crate::IFunction;

#[derive(Clone)]
pub struct SubstringFunction {
    display_name: String
}

impl SubstringFunction {
    pub fn try_create(display_name: &str, ctx: Arc<dyn FunctionCtx>) -> Result<Box<dyn IFunction>> {
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

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], input_rows: usize) -> Result<DataColumnarValue> {
        // TODO: make this function support column value as arguments rather than literal
        let from = columns[1]
            .to_array()?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let end = {
            if columns.len() >= 3 {
                let v = columns[2]
                    .to_array()?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                Some(v.value(0))
            } else {
                None
            }
        };

        if let DataColumnarValue::Constant(from) = from {
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

        let value = columns[0].to_array()?;
        Ok(DataColumnarValue::Array(
            compute::kernels::substring::substring(&value, from, &end)?
        ))
    }
}

impl fmt::Display for SubstringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SUBSTRING({},{},{})", self.expr, self.from, self.len)
    }
}
