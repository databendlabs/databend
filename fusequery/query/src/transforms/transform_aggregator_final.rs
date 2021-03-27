// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::{DataSchemaRef, DataValue};
use common_functions::IFunction;
use common_planners::ExpressionPlan;
use futures::stream::StreamExt;
use log::info;

use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct AggregatorFinalTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl AggregatorFinalTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> FuseQueryResult<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
        }

        Ok(AggregatorFinalTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for AggregatorFinalTransform {
    fn name(&self) -> &str {
        "AggregatorFinalTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let mut funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;

        let start = Instant::now();
        while let Some(block) = stream.next().await {
            let block = block?;
            for (i, func) in funcs.iter_mut().enumerate() {
                if let DataValue::String(Some(ser)) = DataValue::try_from_array(block.column(0), i)?
                {
                    let de: DataValue = serde_json::from_str(&ser)?;
                    if let DataValue::Struct(states) = de {
                        func.merge(&states)?;
                    }
                }
            }
        }
        let delta = start.elapsed();
        info!("Aggregator final cost: {:?}", delta);

        let mut final_results = Vec::with_capacity(funcs.len());
        for func in &funcs {
            final_results.push(func.merge_result()?.to_array(1)?);
        }
        let block = DataBlock::create(self.schema.clone(), final_results);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
