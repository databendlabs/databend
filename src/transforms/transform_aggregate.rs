// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::stream::StreamExt;

use crate::datablocks::DataBlock;
use crate::datastreams::{AggregateStream, DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataSchemaRef, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

#[derive(Debug, Clone)]
pub enum AggregateMode {
    Partial,
    Final,
}

pub struct AggregateTransform {
    mode: AggregateMode,
    funcs: Vec<Function>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl AggregateTransform {
    pub fn try_create(
        mode: AggregateMode,
        schema: DataSchemaRef,
        exprs: Vec<ExpressionPlan>,
    ) -> FuseQueryResult<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
        }

        Ok(AggregateTransform {
            mode,
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    async fn stream(&self) -> FuseQueryResult<SendableDataBlockStream> {
        match self.mode {
            AggregateMode::Partial => Ok(Box::pin(AggregateStream::try_create(
                self.input.execute().await?,
                self.schema.clone(),
                self.funcs.clone(),
            )?)),
            AggregateMode::Final => {
                let mut funcs = self.funcs.clone();
                let mut stream = self.input.execute().await?;
                while let Some(block) = stream.next().await {
                    let block = block?;

                    for (i, func) in funcs.iter_mut().enumerate() {
                        let val = DataValue::try_from_array(block.column(i), 0)?;
                        func.merge(&[val])?;
                    }
                }

                let mut arrays = Vec::with_capacity(funcs.len());
                for func in &funcs {
                    arrays.push(func.result()?.to_array(1)?);
                }
                let block = DataBlock::create(self.schema.clone(), arrays);
                Ok(Box::pin(DataBlockStream::create(
                    self.schema.clone(),
                    None,
                    vec![block],
                )))
            }
        }
    }
}

#[async_trait]
impl IProcessor for AggregateTransform {
    fn name(&self) -> String {
        format!("AggregateTransform({:?})", self.mode)
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        self.stream().await
    }
}
