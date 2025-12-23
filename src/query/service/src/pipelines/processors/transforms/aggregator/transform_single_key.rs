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
use databend_common_expression::ProjectedBlock;
use databend_common_expression::StatesLayout;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_functions::aggregates::StateAddr;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use itertools::Itertools;

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

unsafe impl Send for PartialSingleStateAggregator {}

impl PartialSingleStateAggregator {
    pub fn try_new(params: &Arc<AggregatorParams>) -> Result<Self> {
        let arena = Bump::new();
        let state_layout = params
            .states_layout
            .as_ref()
            .ok_or_else(|| ErrorCode::LayoutError("[TRANSFORM-AGGREGATOR] Layout cannot be None"))?
            .clone();

        let addr: StateAddr = arena.alloc_layout(state_layout.layout).into();
        for (func, loc) in params
            .aggregate_functions
            .iter()
            .zip(state_layout.states_loc.iter())
        {
            func.init_state(AggrState::new(addr, loc));
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

        let meta = block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .copied();

        if let Some(meta) = meta
            && meta.is_agg
        {
            assert_eq!(self.states_layout.num_aggr_func(), meta.num_agg_funcs);
            // Aggregation states are in the back of the block.
            let start = block.num_columns() - self.states_layout.num_aggr_func();
            let states_indices = (start..block.num_columns()).collect::<Vec<_>>();
            let states = ProjectedBlock::project(&states_indices, &block);

            for ((loc, func), state) in self
                .states_layout
                .states_loc
                .iter()
                .zip(self.funcs.iter())
                .zip(states.iter())
            {
                func.batch_merge(&[self.addr], loc, state, None)?;
            }
        } else {
            for ((place, columns), func) in self
                .states_layout
                .states_loc
                .iter()
                .map(|loc| AggrState::new(self.addr, loc))
                .zip(
                    self.arg_indices
                        .iter()
                        .map(|indices| ProjectedBlock::project(indices.as_slice(), &block)),
                )
                .zip(self.funcs.iter())
            {
                func.accumulate(place, columns, None, block.num_rows())?;
            }
        }

        self.rows += block.num_rows();
        self.bytes += block.memory_size();

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        let blocks = if generate_data {
            let mut builders = self.states_layout.serialize_builders(1);

            for ((func, loc), builder) in self
                .funcs
                .iter()
                .zip(self.states_layout.states_loc.iter())
                .zip(builders.iter_mut())
            {
                let builders = builder.as_tuple_mut().unwrap().as_mut_slice();
                func.batch_serialize(&[self.addr], loc, builders)?;
                debug_assert!(builders.iter().map(ColumnBuilder::len).all_equal());
            }

            let columns = builders.into_iter().map(|b| b.build()).collect();
            vec![DataBlock::new_from_columns(columns)]
        } else {
            vec![]
        };

        // destroy states
        for (loc, func) in self.states_layout.states_loc.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(AggrState::new(self.addr, loc)) }
            }
        }

        log::info!(
            "[TRANSFORM-AGGREGATOR] Single key aggregation completed: {} → 1 rows in {:.2}s (real: {:.2}s), throughput: {} rows/sec, {}/sec, total: {}",
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
            .ok_or_else(|| ErrorCode::LayoutError("[TRANSFORM-AGGREGATOR] Layout cannot be None"))?
            .clone();

        assert!(!states_layout.states_loc.is_empty());

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
            self.to_merge_data.push(block);
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if !generate_data {
            return Ok(vec![]);
        }

        let main_addr: StateAddr = self.arena.alloc_layout(self.states_layout.layout).into();

        for (func, loc) in self.funcs.iter().zip(self.states_layout.states_loc.iter()) {
            func.init_state(AggrState::new(main_addr, loc));
        }

        let mut result_builders = self
            .funcs
            .iter()
            .map(|f| Ok(ColumnBuilder::with_capacity(&f.return_type()?, 1)))
            .collect::<Result<Vec<_>>>()?;

        for (idx, ((func, loc), builder)) in self
            .funcs
            .iter()
            .zip(self.states_layout.states_loc.iter())
            .zip(result_builders.iter_mut())
            .enumerate()
        {
            for block in self.to_merge_data.iter() {
                func.batch_merge(&[main_addr], loc, block.get_by_offset(idx), None)?;
            }
            func.merge_result(AggrState::new(main_addr, loc), false, builder)?;
        }

        let columns = result_builders.into_iter().map(|b| b.build()).collect();

        // destroy states
        for (func, loc) in self.funcs.iter().zip(self.states_layout.states_loc.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(AggrState::new(main_addr, loc)) }
            }
        }

        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}
