// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use common_arrow::arrow;
use common_datablocks::DataBlock;
use common_datavalues::{BooleanArray, DataSchema, DataSchemaRef};
use common_functions::IFunction;
use common_planners::ExpressionPlan;
use common_streams::{ExpressionStream, SendableDataBlockStream};

use crate::pipelines::processors::{EmptyProcessor, IProcessor};

pub struct FilterTransform {
    func: Box<dyn IFunction>,
    input: Arc<dyn IProcessor>,
}

impl FilterTransform {
    pub fn try_create(predicate: ExpressionPlan) -> Result<Self> {
        let func = predicate.to_function()?;
        if func.is_aggregator() {
            bail!(
                "Aggregate function {:?} is found in WHERE in query",
                predicate
            );
        }

        Ok(FilterTransform {
            func,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn expression_executor(
        _schema: &DataSchemaRef,
        block: DataBlock,
        funcs: Vec<Box<dyn IFunction>>,
    ) -> Result<DataBlock> {
        let func = funcs[0].clone();
        let result = func.eval(&block)?.to_array(block.num_rows())?;
        let filter_result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::Error::msg("cannot downcast to boolean array"))?;
        DataBlock::try_from_arrow_batch(&arrow::compute::filter_record_batch(
            &block.to_arrow_batch()?,
            filter_result,
        )?)
    }
}

#[async_trait]
impl IProcessor for FilterTransform {
    fn name(&self) -> &str {
        "FilterTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(ExpressionStream::try_create(
            self.input.execute().await?,
            Arc::new(DataSchema::empty()),
            vec![self.func.clone()],
            FilterTransform::expression_executor,
        )?))
    }
}
