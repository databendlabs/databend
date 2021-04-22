// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::Result;
use common_arrow::arrow::compute;
use common_arrow::arrow::compute::CastOptions;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;

use crate::IFunction;

/// provide Datafuse default cast options
pub const DEFAULT_DATAFUSE_CAST_OPTIONS: CastOptions = CastOptions { safe: false };

#[derive(Clone)]
pub struct CastFunction {
    /// The expression to cast
    expr: Box<dyn IFunction>,
    /// The data type to cast to
    cast_type: DataType
}

impl CastFunction {
    pub fn create(expr: Box<dyn IFunction>, cast_type: DataType) -> Box<dyn IFunction> {
        Box::new(Self { expr, cast_type })
    }
}

impl IFunction for CastFunction {
    fn name(&self) -> &str {
        "cast"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        let value = self.expr.eval(block)?;
        match value {
            DataColumnarValue::Array(v) => Ok(DataColumnarValue::Array(
                compute::kernels::cast::cast_with_options(
                    &v,
                    &self.cast_type,
                    &DEFAULT_DATAFUSE_CAST_OPTIONS
                )?
            )),
            DataColumnarValue::Scalar(v) => {
                let scalar_array = v.to_array()?;
                let cast_array = compute::kernels::cast::cast_with_options(
                    &scalar_array,
                    &self.cast_type,
                    &DEFAULT_DATAFUSE_CAST_OPTIONS
                )?;
                let cast_scalar = DataValue::try_from_array(&cast_array, 0)?;
                Ok(DataColumnarValue::Scalar(cast_scalar))
            }
        }
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cast({})", self.expr)
    }
}
