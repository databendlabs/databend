// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ScalarExpressionStream, SendableDataBlockStream};
use crate::datavalues::{BooleanArray, DataSchemaRef};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};
use arrow::compute::filter_record_batch;

pub struct FilterTransform {
    func: Function,
    input: Arc<dyn IProcessor>,
}

impl FilterTransform {
    pub fn try_create(predicate: ExpressionPlan) -> FuseQueryResult<Self> {
        if predicate.has_aggregator() {
            return Err(FuseQueryError::Internal(format!(
                "Aggregate function {:?} is found in WHERE in query",
                predicate
            )));
        }

        let func = predicate.to_function()?;
        Ok(FilterTransform {
            func,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn expression_executor(
        _schema: &DataSchemaRef,
        block: DataBlock,
        funcs: Vec<Function>,
    ) -> FuseQueryResult<DataBlock> {
        let mut func = funcs[0].clone();
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

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        self.input.schema()
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ScalarExpressionStream::try_create(
            self.input.execute().await?,
            self.schema()?,
            vec![self.func.clone()],
            FilterTransform::expression_executor,
        )?))
    }
}
