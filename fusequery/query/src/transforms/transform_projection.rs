// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_functions::IFunction;
use common_planners::ExpressionPlan;

use crate::datastreams::{ExpressionStream, SendableDataBlockStream};
use crate::processors::{EmptyProcessor, IProcessor};

pub struct ProjectionTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl ProjectionTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> Result<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            let func = expr.to_function()?;
            if func.is_aggregator() {
                bail!(
                    "Aggregate function {} is found in ProjectionTransform, should AggregatorTransform",
                    func
                );
            }
            funcs.push(func);
        }

        Ok(ProjectionTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn expression_executor(
        projected_schema: &DataSchemaRef,
        block: DataBlock,
        funcs: Vec<Box<dyn IFunction>>,
    ) -> Result<DataBlock> {
        let mut column_values = Vec::with_capacity(funcs.len());
        for func in funcs {
            column_values.push(func.eval(&block)?.to_array(block.num_rows())?);
        }
        Ok(DataBlock::create(projected_schema.clone(), column_values))
    }
}

#[async_trait]
impl IProcessor for ProjectionTransform {
    fn name(&self) -> &str {
        "ProjectionTransform"
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
            self.schema.clone(),
            self.funcs.clone(),
            ProjectionTransform::expression_executor,
        )?))
    }
}
