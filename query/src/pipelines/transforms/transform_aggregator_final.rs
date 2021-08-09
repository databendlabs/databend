// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_datavalues::prelude::DFBinaryArray;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionRef;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct AggregatorFinalTransform {
    funcs: Vec<AggregateFunctionRef>,
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl AggregatorFinalTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        exprs: Vec<Expression>,
    ) -> Result<Self> {
        let funcs = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function(&schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregatorFinalTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait::async_trait]
impl Processor for AggregatorFinalTransform {
    fn name(&self) -> &str {
        "AggregatorFinalTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        let funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;

        let start = Instant::now();

        let arena = bumpalo::Bump::new();
        let places = funcs
            .iter()
            .map(|func| func.allocate_state(&arena))
            .collect::<Vec<_>>();

        while let Some(block) = stream.next().await {
            let block = block?;
            for (i, func) in funcs.iter().enumerate() {
                let binary_array = block.column(i).to_array()?;
                let binary_array: &DFBinaryArray = binary_array.binary()?;
                let array = binary_array.downcast_ref();

                let place = func.allocate_state(&arena);
                let mut data = array.value(0);
                func.deserialize(place, &mut data)?;
                func.merge(places[i], place)?;
            }
        }
        let delta = start.elapsed();
        tracing::debug!("Aggregator final cost: {:?}", delta);

        let mut final_result = Vec::with_capacity(funcs.len());
        for (idx, func) in funcs.iter().enumerate() {
            let merge_result = func.merge_result(places[idx])?;
            final_result.push(merge_result.to_series_with_size(1)?);
        }

        let mut blocks = vec![];
        if !final_result.is_empty() {
            blocks.push(DataBlock::create_by_array(
                self.schema.clone(),
                final_result,
            ));
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks,
        )))
    }
}
