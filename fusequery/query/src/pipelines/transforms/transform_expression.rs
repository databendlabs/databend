// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_functions::IFunction;
use common_planners::ExpressionPlan;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

// Executes certain expressions over the block.
// The expression consists of column identifiers from the block, constants, common functions.
// For example: hits * 2 + 3.
// ExpressionTransform normally used for transform internal, such as ProjectionTransform.
// Aims to transform a block to another format, such as add one column.
pub struct ExpressionTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>
}

impl ExpressionTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> Result<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            let func = expr.to_function()?;
            if func.is_aggregator() {
                bail!(
                    "Aggregate function {} is found in ExpressionTransform, should AggregatorTransform",
                    func
                );
            }
            funcs.push(func);
        }

        Ok(ExpressionTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create())
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for ExpressionTransform {
    fn name(&self) -> &str {
        "ExpressionTransform"
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
        let projected_schema = self.schema.clone();
        let funcs_clone = self.funcs.clone();

        let input_stream = self.input.execute().await?;
        let stream = input_stream.filter_map(move |v| {
            let block = v.ok()?;
            let rows = block.num_rows();

            let mut column_values = Vec::with_capacity(funcs_clone.len());
            for func in &funcs_clone {
                column_values.push(func.eval(&block).ok()?.to_array(rows).ok()?);
            }
            let block = DataBlock::create(projected_schema.clone(), column_values);
            Some(Ok(block))
        });

        Ok(Box::pin(stream))
    }
}
