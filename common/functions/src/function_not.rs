// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct NotFunction {
    depth: usize,
    func: Box<dyn IFunction>
}

impl NotFunction {
    pub fn try_create(func: Box<dyn IFunction>) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(NotFunction { depth: 0, func }))
    }
}

impl IFunction for NotFunction {
    fn name(&self) -> &str {
        "NotFunction"
    }

    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        self.func.return_type(input_schema)
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.func.nullable(input_schema)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        self.func.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        self.func.accumulate(block)
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        self.func.accumulate_result()
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        self.func.merge(states)
    }

    fn merge_result(&self) -> Result<DataValue> {
        self.func.merge_result()
    }

    fn is_aggregator(&self) -> bool {
        self.func.is_aggregator()
    }
}

impl fmt::Display for NotFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", "NOT".to_string())
    }
}
