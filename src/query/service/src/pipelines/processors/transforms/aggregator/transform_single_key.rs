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
use std::borrow::BorrowMut;
use std::sync::Arc;
use std::vec;

use bumpalo::Bump;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::Value;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::aggregates::StateAddr;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::transform_accumulating::AccumulatingTransform;
use common_pipeline_transforms::processors::transforms::transform_accumulating::AccumulatingTransformer;

use crate::pipelines::processors::AggregatorParams;

/// SELECT COUNT | SUM FROM table;
pub struct PartialSingleStateAggregator {
    #[allow(dead_code)]
    arena: Bump,
    places: Vec<StateAddr>,
    arg_indices: Vec<Vec<usize>>,
    funcs: Vec<AggregateFunctionRef>,
}

impl PartialSingleStateAggregator {
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

        let place: StateAddr = arena.alloc_layout(layout).into();
        let mut places = Vec::with_capacity(params.offsets_aggregate_states.len());

        for (idx, func) in params.aggregate_functions.iter().enumerate() {
            let arg_place = place.next(params.offsets_aggregate_states[idx]);
            func.init_state(arg_place);
            places.push(arg_place);
        }

        Ok(AccumulatingTransformer::create(
            input,
            output,
            PartialSingleStateAggregator {
                arena,
                places,
                funcs: params.aggregate_functions.clone(),
                arg_indices: params.aggregate_functions_arguments.clone(),
            },
        ))
    }
}

impl AccumulatingTransform for PartialSingleStateAggregator {
    const NAME: &'static str = "AggregatorPartialTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let block = block.convert_to_full();

        for (idx, func) in self.funcs.iter().enumerate() {
            let mut arg_columns = vec![];
            for index in self.arg_indices[idx].iter() {
                arg_columns.push(
                    block
                        .get_by_offset(*index)
                        .value
                        .as_column()
                        .unwrap()
                        .clone(),
                );
            }
            let place = self.places[idx];
            func.accumulate(place, &arg_columns, None, block.num_rows())?;
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        let mut generate_data_block = vec![];

        if generate_data {
            let mut columns = Vec::with_capacity(self.funcs.len());

            for (idx, func) in self.funcs.iter().enumerate() {
                let place = self.places[idx];

                let mut data = Vec::with_capacity(4);
                func.serialize(place, &mut data)?;
                columns.push(BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(data)),
                });
            }

            generate_data_block = vec![DataBlock::new(columns, 1)];
        }

        // destroy states
        for (place, func) in self.places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(*place) }
            }
        }

        Ok(generate_data_block)
    }
}

/// SELECT COUNT | SUM FROM table;
pub struct FinalSingleStateAggregator {
    arena: Bump,
    layout: Layout,
    to_merge_places: Vec<Vec<StateAddr>>,
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
                to_merge_places: vec![vec![]; params.aggregate_functions.len()],
                offsets_aggregate_states: params.offsets_aggregate_states.clone(),
            },
        ))
    }

    fn new_places(&self) -> Vec<StateAddr> {
        let place: StateAddr = self.arena.alloc_layout(self.layout).into();
        self.funcs
            .iter()
            .enumerate()
            .map(|(idx, func)| {
                let arg_place = place.next(self.offsets_aggregate_states[idx]);
                func.init_state(arg_place);
                arg_place
            })
            .collect()
    }
}

impl AccumulatingTransform for FinalSingleStateAggregator {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if !block.is_empty() {
            let block = block.convert_to_full();
            let places = self.new_places();

            for (index, func) in self.funcs.iter().enumerate() {
                let binary_array = block.get_by_offset(index).value.as_column().unwrap();
                let binary_array = binary_array.as_string().ok_or_else(|| {
                    ErrorCode::IllegalDataType("binary array should be string type")
                })?;

                let mut data = unsafe { binary_array.index_unchecked(0) };
                func.deserialize(places[index], &mut data)?;
                self.to_merge_places[index].push(places[index]);
            }
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        let mut generate_data_block = vec![];

        if generate_data {
            let mut aggr_values = {
                let mut builders = vec![];
                for func in &self.funcs {
                    let data_type = func.return_type()?;
                    builders.push(ColumnBuilder::with_capacity(&data_type, 1));
                }
                builders
            };

            let main_places = self.new_places();
            for (index, func) in self.funcs.iter().enumerate() {
                let main_place = main_places[index];

                for place in self.to_merge_places[index].iter() {
                    func.merge(main_place, *place)?;
                }

                let array = aggr_values[index].borrow_mut();
                func.merge_result(main_place, array)?;
            }

            let mut columns = Vec::with_capacity(self.funcs.len());
            for builder in aggr_values {
                columns.push(builder.build());
            }

            // destroy states
            for (place, func) in main_places.iter().zip(self.funcs.iter()) {
                if func.need_manual_drop_state() {
                    unsafe { func.drop_state(*place) }
                }
            }

            generate_data_block = vec![DataBlock::new_from_columns(columns)];
        }

        for (places, func) in self.to_merge_places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                for place in places {
                    unsafe { func.drop_state(*place) }
                }
            }
        }

        Ok(generate_data_block)
    }
}
