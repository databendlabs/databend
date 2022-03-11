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
use std::borrow::BorrowMut;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::MutableColumn;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::aggregates::StateAddr;
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

    #[tracing::instrument(level = "debug", name = "aggregator_final_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        let funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;

        let start = Instant::now();
        let arena = bumpalo::Bump::new();

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
            for (idx, func) in funcs.iter().enumerate() {
                let place = places[idx].into();

                let binary_array = block.column(idx);
                let binary_array: &StringColumn = Series::check_get(binary_array)?;

                let mut data = binary_array.get_data(0);
                let s = funcs[idx].state_layout();
                let temp = arena.alloc_layout(s);
                let temp_addr = temp.into();
                funcs[idx].init_state(temp_addr);

                func.deserialize(temp_addr, &mut data)?;
                func.merge(place, temp_addr)?;
            }
        }
        let delta = start.elapsed();
        tracing::debug!("Aggregator final cost: {:?}", delta);

        let funcs_len = funcs.len();

        let mut aggr_values: Vec<Box<dyn MutableColumn>> = {
            let mut builders = vec![];
            for func in &funcs {
                let data_type = func.return_type()?;
                let builder = data_type.create_mutable(1024);
                builders.push(builder)
            }
            builders
        };

        for (idx, func) in funcs.iter().enumerate() {
            let place = places[idx].into();
            let array: &mut dyn MutableColumn = aggr_values[idx].borrow_mut();
            let _ = func.merge_result(place, array)?;
        }

        let mut columns = Vec::with_capacity(funcs_len);
        for mut array in aggr_values {
            let col = array.to_column();
            columns.push(col);
        }
        let mut blocks = vec![];
        if !columns.is_empty() {
            blocks.push(DataBlock::create(self.schema.clone(), columns));
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks,
        )))
    }
}
