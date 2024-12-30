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

use std::alloc::Layout;
use std::sync::Arc;
use std::time::Instant;
use std::vec;

use bumpalo::Bump;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
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
    offsets: Vec<usize>,
    arg_indices: Vec<Vec<usize>>,
    funcs: Vec<AggregateFunctionRef>,

    start: Instant,
    first_block_start: Option<Instant>,
    rows: usize,
    bytes: usize,
}

impl PartialSingleStateAggregator {
    pub fn try_new(params: &Arc<AggregatorParams>) -> Result<Self> {
        assert!(!params.offsets_aggregate_states.is_empty());

        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        let addr: StateAddr = arena.alloc_layout(layout).into();
        for (func, &offset) in params
            .aggregate_functions
            .iter()
            .zip(params.offsets_aggregate_states.iter())
        {
            func.init_state(&AggrState::with_offset(addr, offset));
        }

        Ok(PartialSingleStateAggregator {
            arena,
            addr,
            offsets: params.offsets_aggregate_states.clone(),
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
            for ((place, agg_state), func) in self
                .offsets
                .iter()
                .copied()
                .enumerate()
                .map(|(idx, offset)| {
                    let place = AggrState::with_offset(self.addr, offset);
                    // Aggregation states are in the back of the block.
                    let agg_index = block.num_columns() - self.funcs.len() + idx;
                    (
                        place,
                        block.get_by_offset(agg_index).value.as_column().unwrap(),
                    )
                })
                .zip(self.funcs.iter())
            {
                func.batch_merge_single(&place, agg_state)?;
            }
        } else {
            for ((place, columns), func) in self
                .offsets
                .iter()
                .copied()
                .map(|offset| AggrState::with_offset(self.addr, offset))
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
            let columns = self
                .funcs
                .iter()
                .zip(
                    self.offsets
                        .iter()
                        .map(|&offset| AggrState::with_offset(self.addr, offset)),
                )
                .map(|(func, place)| {
                    let mut data = Vec::new();
                    func.serialize(&place, &mut data)?;

                    Ok(BlockEntry::new(
                        DataType::Binary,
                        Value::Scalar(Scalar::Binary(data)),
                    ))
                })
                .collect::<Result<_>>()?;

            vec![DataBlock::new(columns, 1)]
        } else {
            vec![]
        };

        // destroy states
        for (&offset, func) in self.offsets.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(&AggrState::with_offset(self.addr, offset)) }
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
    layout: Layout,
    to_merge_data: Vec<Vec<Column>>,
    funcs: Vec<AggregateFunctionRef>,
    offsets_aggregate_states: Vec<usize>,
}

impl FinalSingleStateAggregator {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        assert!(!params.offsets_aggregate_states.is_empty());

        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        Ok(AccumulatingTransformer::create(
            input,
            output,
            FinalSingleStateAggregator {
                arena,
                layout,
                funcs: params.aggregate_functions.clone(),
                to_merge_data: vec![vec![]; params.aggregate_functions.len()],
                offsets_aggregate_states: params.offsets_aggregate_states.clone(),
            },
        ))
    }
}

impl AccumulatingTransform for FinalSingleStateAggregator {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if !block.is_empty() {
            let block = block.consume_convert_to_full();

            for (index, _) in self.funcs.iter().enumerate() {
                let binary_array = block.get_by_offset(index).value.as_column().unwrap();
                self.to_merge_data[index].push(binary_array.clone());
            }
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        if !generate_data {
            return Ok(vec![]);
        }

        let mut aggr_values = {
            let mut builders = vec![];
            for func in &self.funcs {
                let data_type = func.return_type()?;
                builders.push(ColumnBuilder::with_capacity(&data_type, 1));
            }
            builders
        };

        // let main_places = self.new_places();
        let main_addr: StateAddr = self.arena.alloc_layout(self.layout).into();

        let main_places = self
            .funcs
            .iter()
            .zip(
                self.offsets_aggregate_states
                    .iter()
                    .map(|&offset| AggrState::with_offset(main_addr, offset)),
            )
            .map(|(func, place)| {
                func.init_state(&place);
                place
            })
            .collect::<Vec<_>>();

        for (((func, place), to_merge_data), builder) in self
            .funcs
            .iter()
            .zip(main_places.iter())
            .zip(self.to_merge_data.iter())
            .zip(aggr_values.iter_mut())
        {
            for col in to_merge_data {
                func.batch_merge_single(place, col)?;
            }
            func.merge_result(place, builder)?;
        }

        let columns = aggr_values.into_iter().map(|b| b.build()).collect();

        // destroy states
        for (place, func) in main_places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(place) }
            }
        }

        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}
