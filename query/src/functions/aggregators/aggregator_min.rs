// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use fuse_query_datavalues::{
    self as datavalues, DataColumnarValue, DataSchema, DataType, DataValue,
    DataValueAggregateOperator,
};

use crate::datablocks::DataBlock;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;
use crate::sessions::FuseQueryContextRef;

#[derive(Clone)]
pub struct AggregatorMinFunction {
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorMinFunction {
    pub fn try_create(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        if args.len() != 1 {
            return Err(FuseQueryError::build_internal_error(
                "Aggregator function Min args require single argument".to_string(),
            ));
        }

        Ok(Box::new(AggregatorMinFunction {
            depth: 0,
            arg: args[0].clone(),
            state: DataValue::Null,
        }))
    }
}

impl IFunction for AggregatorMinFunction {
    fn return_type(&self, input_schema: &DataSchema) -> FuseQueryResult<DataType> {
        self.arg.return_type(input_schema)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> FuseQueryResult<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> FuseQueryResult<DataColumnarValue> {
        self.arg.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> FuseQueryResult<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;
        self.state = datavalues::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            datavalues::data_array_aggregate_op(
                DataValueAggregateOperator::Min,
                val.to_array(rows)?,
            )?,
        )?;
        Ok(())
    }

    fn accumulate_result(&self) -> FuseQueryResult<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> FuseQueryResult<()> {
        let val = states[self.depth].clone();
        self.state = datavalues::data_value_aggregate_op(
            DataValueAggregateOperator::Min,
            self.state.clone(),
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> FuseQueryResult<DataValue> {
        Ok(self.state.clone())
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Min({})", self.arg)
    }
}
