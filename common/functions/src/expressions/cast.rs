// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_arrow::arrow::compute;
use common_arrow::arrow::compute::CastOptions;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::function::IFunction;

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
    pub fn create(cast_type: DataType) -> Box<dyn IFunction> {
        Box::new(Self { expr, cast_type })
    }
}

impl IFunction for CastFunction {
    fn name(&self) -> &str {
        "cast"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        let value = columns[0].to_array()?;
        Ok(DataColumnarValue::Array(
            compute::kernels::cast::cast_with_options(
                &value,
                &self.cast_type,
                &DEFAULT_DATAFUSE_CAST_OPTIONS
            )?
        ))
    }
}
    }

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST({})", self.expr)
    }
}
