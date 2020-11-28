// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ScalarExpressionStream, SendableDataBlockStream};
use crate::datavalues::BooleanArray;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};
use arrow::compute::filter_record_batch;

pub struct FilterTransform {
    pub predicate: ExpressionPlan,
    input: Arc<dyn IProcessor>,
}

impl FilterTransform {
    pub fn try_create(predicate: ExpressionPlan) -> FuseQueryResult<Self> {
        Ok(FilterTransform {
            predicate,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn expression_executor(block: DataBlock, mut func: Function) -> FuseQueryResult<DataBlock> {
        func.eval(&block)?;
        let result = func.result()?.to_array(block.num_rows())?;
        let filter_array = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                FuseQueryError::Internal("cannot downcast to boolean array".to_string())
            })?;
        Ok(DataBlock::try_from_arrow_batch(&filter_record_batch(
            &block.to_arrow_batch()?,
            filter_array,
        )?)?)
    }
}

#[async_trait]
impl IProcessor for FilterTransform {
    fn name(&self) -> &'static str {
        "FilterTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        self.input = input;
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ScalarExpressionStream::try_create(
            self.input.execute().await?,
            self.predicate.to_function()?,
            FilterTransform::expression_executor,
        )?))
    }
}
