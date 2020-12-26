// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ExpressionStream, SendableDataBlockStream};
use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct ProjectionTransform {
    funcs: Vec<Function>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl ProjectionTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> FuseQueryResult<Self> {
        for expr in &exprs {
            if expr.is_aggregate() {
                return Err(FuseQueryError::Internal(format!(
                    "Unsupported aggregator function: {:?}",
                    expr
                )));
            }
        }

        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
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
        funcs: Vec<Function>,
    ) -> FuseQueryResult<DataBlock> {
        let mut column_values = Vec::with_capacity(funcs.len());
        for mut func in funcs {
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

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ExpressionStream::try_create(
            self.input.execute().await?,
            self.schema.clone(),
            self.funcs.clone(),
            ProjectionTransform::expression_executor,
        )?))
    }
}
