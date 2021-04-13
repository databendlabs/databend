// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::anyhow;
use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;

use crate::IFunction;

#[derive(Clone, Debug)]
pub struct ColumnFunction {
    value: String,
    saved: Option<DataValue>,
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
    fn name(&self) -> &str {
        "ColumnFunction"
    }

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

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        if self.saved.is_none() {
            let array = block.column_by_name(self.value.as_str())?;
            let first = DataValue::try_from_array(array, 0)?;
            self.saved = Some(first);
        }
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        let saved = self
            .saved
            .as_ref()
            .ok_or_else(|| anyhow!("column saved is None"))?;
        Ok(vec![saved.clone()])
    }

    fn merge(&mut self, _states: &[DataValue]) -> Result<()> {
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        let saved = self
            .saved
            .as_ref()
            .ok_or_else(|| anyhow!("column saved is None"))?;
        Ok(saved.clone())
    }
}

impl fmt::Display for ColumnFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
