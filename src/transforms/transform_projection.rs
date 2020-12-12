// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::debug;
use std::time::Instant;

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
            funcs.push(expr.to_function(0)?);
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
        let mut arrays = Vec::with_capacity(funcs.len());

        let start = Instant::now();
        for mut func in funcs {
            arrays.push(func.eval(&block)?.to_array(block.num_rows())?);
        }
        let duration = start.elapsed();
        debug!("transform projection cost:{:?}", duration);

        Ok(DataBlock::create(projected_schema.clone(), arrays))
    }
}

#[async_trait]
impl IProcessor for ProjectionTransform {
    fn name(&self) -> String {
        "ProjectionTransform".to_owned()
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
