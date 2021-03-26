// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::{DataColumnarValue, DataSchema, DataType, DataValue};

use crate::{FunctionResult, IFunction};

#[derive(Clone)]
pub struct AliasFunction {
    depth: usize,
    alias: String,
    func: Box<dyn IFunction>,
}

impl AliasFunction {
    pub fn try_create(
        alias: String,
        func: Box<dyn IFunction>,
    ) -> FunctionResult<Box<dyn IFunction>> {
        Ok(Box::new(AliasFunction {
            depth: 0,
            alias,
            func,
        }))
    }
}

impl IFunction for AliasFunction {
    fn return_type(&self, input_schema: &DataSchema) -> FunctionResult<DataType> {
        self.func.return_type(input_schema)
    }

    fn nullable(&self, input_schema: &DataSchema) -> FunctionResult<bool> {
        self.func.nullable(input_schema)
    }

    fn eval(&self, block: &DataBlock) -> FunctionResult<DataColumnarValue> {
        self.func.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> FunctionResult<()> {
        self.func.accumulate(block)
    }

    fn accumulate_result(&self) -> FunctionResult<Vec<DataValue>> {
        self.func.accumulate_result()
    }

    fn merge(&mut self, states: &[DataValue]) -> FunctionResult<()> {
        self.func.merge(states)
    }

    fn merge_result(&self) -> FunctionResult<DataValue> {
        self.func.merge_result()
    }

    fn is_aggregator(&self) -> bool {
        self.func.is_aggregator()
    }
}

impl fmt::Display for AliasFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.alias)
    }
}
