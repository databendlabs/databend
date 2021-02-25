// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ExpressionStream, SendableDataBlockStream};
use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};
use crate::sessions::FuseQueryContextRef;

pub struct ProjectionTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl ProjectionTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        exprs: Vec<ExpressionPlan>,
    ) -> FuseQueryResult<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            let func = expr.to_function(ctx.clone())?;
            if func.is_aggregator() {
                return Err(FuseQueryError::Internal(format!(
                    "Aggregate function {} is found in ProjectionTransform, should AggregatorTransform",
                    func
                )));
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
    ) -> FuseQueryResult<DataBlock> {
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

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ExpressionStream::try_create(
            self.input.execute().await?,
            self.schema.clone(),
            self.funcs.clone(),
            ProjectionTransform::expression_executor,
        )?))
    }
}
