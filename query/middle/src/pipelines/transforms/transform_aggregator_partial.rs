// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::aggregates::StateAddr;
use common_io::prelude::*;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct AggregatorPartialTransform {
    funcs: Vec<AggregateFunctionRef>,
    arg_names: Vec<Vec<String>>,

    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl AggregatorPartialTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        exprs: Vec<Expression>,
    ) -> Result<Self> {
        let funcs = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function(&schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;

        let arg_names = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function_names())
            .collect::<Result<Vec<_>>>()?;

        Ok(AggregatorPartialTransform {
            funcs,
            arg_names,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait::async_trait]
impl Processor for AggregatorPartialTransform {
    fn name(&self) -> &str {
        "AggregatorPartialTransform"
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

    #[tracing::instrument(level = "debug", name = "aggregator_partial_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");
        let start = Instant::now();

        let funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;
        let arg_names = self.arg_names.clone();

        let arena = Bump::new();

        let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&funcs) };

        let places: Vec<usize> = {
            let place: StateAddr = arena.alloc_layout(layout).into();
            funcs
                .iter()
                .enumerate()
                .map(|(idx, func)| {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.init_state(arg_place);
                    arg_place.addr()
                })
                .collect()
        };

        while let Some(block) = stream.next().await {
            let block = block?;
            let rows = block.num_rows();

            for (idx, func) in funcs.iter().enumerate() {
                let mut arg_columns = vec![];
                for name in arg_names[idx].iter() {
                    arg_columns.push(block.try_column_by_name(name)?.clone());
                }
                let place = places[idx].into();
                func.accumulate(place, &arg_columns, None, rows)?;
            }
        }
        let delta = start.elapsed();
        tracing::debug!("Aggregator partial cost: {:?}", delta);

        let mut columns = Vec::with_capacity(funcs.len());
        let mut bytes = BytesMut::new();

        for (idx, func) in funcs.iter().enumerate() {
            let place = places[idx].into();
            func.serialize(place, &mut bytes)?;
            let mut array_builder = MutableStringColumn::with_capacity(4);
            array_builder.append_value(&bytes[..]);
            bytes.clear();
            let col = array_builder.to_column();
            columns.push(col);
        }

        let block = DataBlock::create(self.schema.clone(), columns);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
