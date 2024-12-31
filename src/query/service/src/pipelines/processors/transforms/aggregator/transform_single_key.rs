// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;
use std::time::Instant;
use std::vec;

use bumpalo::Bump;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::InputColumns;
use databend_common_expression::StatesLayout;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_functions::aggregates::StateAddr;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

/// SELECT COUNT | SUM FROM table;
pub struct PartialSingleStateAggregator {
    #[allow(dead_code)]
    arena: Bump,
    addr: StateAddr,
    states_layout: StatesLayout,
    arg_indices: Vec<Vec<usize>>,
    funcs: Vec<AggregateFunctionRef>,

    start: Instant,
    first_block_start: Option<Instant>,
    rows: usize,
    bytes: usize,
}

impl PartialSingleStateAggregator {
    pub fn try_new(params: &Arc<AggregatorParams>) -> Result<Self> {
        let arena = Bump::new();
        let state_layout = params
            .states_layout
            .as_ref()
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?
            .clone();

        let addr: StateAddr = arena.alloc_layout(state_layout.layout).into();
        for (func, loc) in params
            .aggregate_functions
            .iter()
            .zip(state_layout.loc.iter().cloned())
        {
            func.init_state(&AggrState::with_loc(addr, loc));
        }

        Ok(PartialSingleStateAggregator {
            arena,
            addr,
            states_layout: state_layout,
            funcs: params.aggregate_functions.clone(),
            arg_indices: params.aggregate_functions_arguments.clone(),
            start: Instant::now(),
            first_block_start: None,
            rows: 0,
            bytes: 0,
        })
    }
}

impl AccumulatingTransform for PartialSingleStateAggregator {
    const NAME: &'static str = "AggregatorPartialTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

        let is_agg_index_block = block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default();

        let block = block.consume_convert_to_full();

        if is_agg_index_block {
            // Aggregation states are in the back of the block.
            let states_indices = (block.num_columns() - self.states_layout.states_count()
                ..block.num_columns())
                .collect::<Vec<_>>();
            let states = InputColumns::new_block_proxy(&states_indices, &block);

            for (place, func) in self
                .states_layout
                .loc
                .iter()
                .cloned()
                .map(|loc| AggrState::with_loc(self.addr, loc))
                .zip(self.funcs.iter())
            {
                func.batch_merge_single(&place, states)?;
            }
        } else {
            for ((place, columns), func) in self
                .states_layout
                .loc
                .iter()
                .cloned()
                .map(|loc| AggrState::with_loc(self.addr, loc))
                .zip(
                    self.arg_indices
                        .iter()
                        .map(|indices| InputColumns::new_block_proxy(indices.as_slice(), &block)),
                )
                .zip(self.funcs.iter())
            {
                func.accumulate(&place, columns, None, block.num_rows())?;
            }
        }

        self.rows += block.num_rows();
        self.bytes += block.memory_size();

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        let blocks = if generate_data {
            let mut builders = self.states_layout.serialize_builders(1);

            self.funcs
                .iter()
                .zip(
                    self.states_layout
                        .loc
                        .iter()
                        .cloned()
                        .map(|loc| AggrState::with_loc(self.addr, loc)),
                )
                .try_for_each(|(func, place)| func.serialize_builder(&place, &mut builders))?;

            let columns = builders.into_iter().map(|b| b.build()).collect();
            vec![DataBlock::new_from_columns(columns)]
        } else {
            vec![]
        };

        // destroy states
        for (loc, func) in self
            .states_layout
            .loc
            .iter()
            .cloned()
            .zip(self.funcs.iter())
        {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(&AggrState::with_loc(self.addr, loc)) }
            }
        }

        log::info!(
            "Aggregated {} to 1 rows in {} sec (real: {}). ({} rows/sec, {}/sec, {})",
            self.rows,
            self.start.elapsed().as_secs_f64(),
            if let Some(t) = &self.first_block_start {
                t.elapsed().as_secs_f64()
            } else {
                self.start.elapsed().as_secs_f64()
            },
            convert_number_size(self.rows as f64 / self.start.elapsed().as_secs_f64()),
            convert_byte_size(self.bytes as f64 / self.start.elapsed().as_secs_f64()),
            convert_byte_size(self.bytes as _),
        );

        Ok(blocks)
    }
}

/// SELECT COUNT | SUM FROM table;
pub struct FinalSingleStateAggregator {
    arena: Bump,
    states_layout: StatesLayout,
    to_merge_data: Vec<DataBlock>,
    funcs: Vec<AggregateFunctionRef>,
}

impl FinalSingleStateAggregator {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let arena = Bump::new();
        let states_layout = params
            .states_layout
            .as_ref()
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?
            .clone();

        assert!(!states_layout.loc.is_empty());

        Ok(AccumulatingTransformer::create(
            input,
            output,
            FinalSingleStateAggregator {
                arena,
                states_layout,
                funcs: params.aggregate_functions.clone(),
                to_merge_data: Vec::new(),
            },
        ))
    }
}

impl AccumulatingTransform for FinalSingleStateAggregator {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if !block.is_empty() {
            let block = block.consume_convert_to_full();
            self.to_merge_data.push(block);
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if !generate_data {
            return Ok(vec![]);
        }

        let main_addr: StateAddr = self.arena.alloc_layout(self.states_layout.layout).into();

        let main_places = self
            .funcs
            .iter()
            .zip(
                self.states_layout
                    .loc
                    .iter()
                    .cloned()
                    .map(|loc| AggrState::with_loc(main_addr, loc)),
            )
            .map(|(func, place)| {
                func.init_state(&place);
                place
            })
            .collect::<Vec<_>>();

        let mut result_builders = self
            .funcs
            .iter()
            .map(|f| Ok(ColumnBuilder::with_capacity(&f.return_type()?, 1)))
            .collect::<Result<Vec<_>>>()?;

        let states_indices = (0..self.states_layout.states_count()).collect::<Vec<_>>();
        for ((func, place), builder) in self
            .funcs
            .iter()
            .zip(main_places.iter())
            .zip(result_builders.iter_mut())
        {
            for block in self.to_merge_data.iter() {
                let states = InputColumns::new_block_proxy(&states_indices, block);
                func.batch_merge_single(place, states)?;
            }
            func.merge_result(place, builder)?;
        }

        let columns = result_builders.into_iter().map(|b| b.build()).collect();

        // destroy states
        for (place, func) in main_places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(place) }
            }
        }

        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}
