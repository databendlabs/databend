// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::{DataField, DataSchema, DataSchemaRef, DataType, DataValue, StringArray};
use common_functions::IFunction;
use common_planners::ExpressionPlan;
use futures::stream::StreamExt;
use log::info;

use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::processors::{EmptyProcessor, IProcessor};

pub struct AggregatorPartialTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl AggregatorPartialTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> Result<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
        }

        Ok(AggregatorPartialTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for AggregatorPartialTransform {
    fn name(&self) -> &str {
        "AggregatorPartialTransform"
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
        let mut funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;

        let start = Instant::now();
        while let Some(block) = stream.next().await {
            let block = block?;

            for func in funcs.iter_mut() {
                func.accumulate(&block)?;
            }
        }
        let delta = start.elapsed();
        info!("Aggregator partial cost: {:?}", delta);

        let mut acc_results = Vec::with_capacity(funcs.len());
        for func in &funcs {
            let states = DataValue::Struct(func.accumulate_result()?);
            let serialized = serde_json::to_string(&states)?;
            acc_results.push(serialized);
        }

        let partial_schema = Arc::new(DataSchema::new(vec![DataField::new(
            "partial_result",
            DataType::Utf8,
            false,
        )]));
        let partial_results = acc_results
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<&str>>();
        let block = DataBlock::create(
            partial_schema,
            vec![Arc::new(StringArray::from(partial_results))],
        );
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
