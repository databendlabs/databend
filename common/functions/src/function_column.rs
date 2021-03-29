// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::{bail, Result};
use common_datablocks::DataBlock;
use common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::IFunction;

#[derive(Clone, Debug)]
pub struct ColumnFunction {
    value: String,
    saved: Option<DataColumnarValue>,
}

impl ColumnFunction {
    pub fn try_create(value: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(ColumnFunction {
            value: value.to_string(),
            saved: None,
        }))
    }
}

impl IFunction for ColumnFunction {
    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        let field = if self.value == "*" {
            input_schema.field(0)
        } else {
            input_schema.field_with_name(self.value.as_str())?
        };

        Ok(field.data_type().clone())
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        let field = if self.value == "*" {
            input_schema.field(0)
        } else {
            input_schema.field_with_name(self.value.as_str())?
        };
        Ok(field.is_nullable())
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Array(
            block.column_by_name(self.value.as_str())?.clone(),
        ))
    }

    fn set_depth(&mut self, _depth: usize) {}

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        self.saved = Some(DataColumnarValue::Array(
            block.column_by_name(self.value.as_str())?.clone(),
        ));
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        bail!("Unsupported accumulate_result for function field");
    }

    fn merge(&mut self, _states: &[DataValue]) -> Result<()> {
        bail!("Unsupported merge for function field");
    }

    fn merge_result(&self) -> Result<DataValue> {
        bail!("Unsupported merge_result for function field");
    }
}

impl fmt::Display for ColumnFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
