// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;

use crate::IFunction;

#[derive(Clone, Debug)]
pub struct LiteralFunction {
    value: DataValue
}

impl LiteralFunction {
    pub fn try_create(value: DataValue) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(LiteralFunction { value }))
    }
}

impl IFunction for LiteralFunction {
    fn name(&self) -> &str {
        "LiteralFunction"
    }

    fn return_type(&self, _input_schema: &DataSchema) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _block: &DataBlock) -> Result<DataColumnarValue> {
        Ok(DataColumnarValue::Scalar(self.value.clone()))
    }

    // For aggregate wrapper: sum(a+2) + 1
    fn accumulate(&mut self, _block: &DataBlock) -> Result<()> {
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.value.clone()])
    }

    fn merge(&mut self, _states: &[DataValue]) -> Result<()> {
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.value.clone())
    }
}

impl fmt::Display for LiteralFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
