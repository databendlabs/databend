// Copyright 2022 Datafuse Labs.
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
use common_base::runtime::ThreadPool;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::MutableColumn;
use common_datavalues::MutableStringColumn;
use common_datavalues::ScalarColumn;
use common_datavalues::ScalarColumnBuilder;
use common_datavalues::Series;
use common_datavalues::StringColumn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::aggregates::StateAddr;

use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;

pub type FinalSingleStateAggregator = SingleStateAggregator<true>;
pub type PartialSingleStateAggregator = SingleStateAggregator<false>;

/// SELECT COUNT | SUM FROM table;
pub struct SingleStateAggregator<const FINAL: bool> {
    funcs: Vec<AggregateFunctionRef>,
    arg_indices: Vec<Vec<usize>>,
    schema: DataSchemaRef,
    arena: Bump,

    places: Vec<StateAddr>,
    to_merge_places: Vec<Vec<StateAddr>>,
    layout: Layout,
    offsets_aggregate_states: Vec<usize>,
    max_threads: usize,
    states_dropped: bool,
}

impl<const FINAL: bool> SingleStateAggregator<FINAL> {
    pub fn try_create(params: &Arc<AggregatorParams>, max_threads: usize) -> Result<Self> {
        assert!(!params.offsets_aggregate_states.is_empty());
        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        let get_places = || -> Vec<StateAddr> {
            let place: StateAddr = arena.alloc_layout(layout).into();
            params
                .aggregate_functions
                .iter()
                .enumerate()
                .map(|(idx, func)| {
                    let arg_place = place.next(params.offsets_aggregate_states[idx]);
                    func.init_state(arg_place);
                    arg_place
                })
                .collect()
        };
        let places = get_places();

        Ok(Self {
            arena,
            places,
            to_merge_places: vec![vec![]; params.aggregate_functions.len()],
            funcs: params.aggregate_functions.clone(),
            arg_indices: params.aggregate_functions_arguments.clone(),
            schema: params.output_schema.clone(),
            layout,
            offsets_aggregate_states: params.offsets_aggregate_states.clone(),
            states_dropped: false,
            max_threads,
        })
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

    fn drop_states(&mut self) {
        if !self.states_dropped {
            for (place, func) in self.places.iter().zip(self.funcs.iter()) {
                if func.need_manual_drop_state() {
                    unsafe { func.drop_state(*place) }
                }
            }

            for (places, func) in self.to_merge_places.iter().zip(self.funcs.iter()) {
                if func.need_manual_drop_state() {
                    for place in places {
                        unsafe { func.drop_state(*place) }
                    }
                }
            }

            self.states_dropped = true;
        }
    }
}

impl Aggregator for SingleStateAggregator<true> {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        let places = self.new_places();
        for (index, func) in self.funcs.iter().enumerate() {
            let binary_array = block.column(index);
            let binary_array: &StringColumn = Series::check_get(binary_array)?;
            let mut data = binary_array.get_data(0);
            func.deserialize(places[index], &mut data)?;
            self.to_merge_places[index].push(places[index]);
        }
        Ok(())
    }

    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        let mut aggr_values: Vec<Box<dyn MutableColumn>> = {
            let mut builders = vec![];
            for func in &self.funcs {
                let data_type = func.return_type()?;
                builders.push(data_type.create_mutable(1));
            }
            builders
        };

        let mut thread_pool = ThreadPool::create(self.max_threads)?;

        for (index, func) in self.funcs.iter().enumerate() {
            let main_place = self.places[index];

            if func.support_merge_parallel() {
                // parallel_merge
                for place in self.to_merge_places[index].iter() {
                    func.merge_parallel(&mut thread_pool, main_place, *place)?;
                }
            } else {
                for place in self.to_merge_places[index].iter() {
                    func.merge(main_place, *place)?;
                }
            }

            let array: &mut dyn MutableColumn = aggr_values[index].borrow_mut();
            func.merge_result(main_place, array)?;
        }

        let mut columns: Vec<ColumnRef> = Vec::with_capacity(self.funcs.len());
        for mut array in aggr_values {
            columns.push(array.to_column());
        }
        Ok(vec![DataBlock::create(self.schema.clone(), columns)])
    }
}

impl Aggregator for SingleStateAggregator<false> {
    const NAME: &'static str = "AggregatorPartialTransform";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let rows = block.num_rows();
        for (idx, func) in self.funcs.iter().enumerate() {
            let mut arg_columns = vec![];
            for index in self.arg_indices[idx].iter() {
                arg_columns.push(block.column(*index).clone());
            }
            let place = self.places[idx];
            func.accumulate(place, &arg_columns, None, rows)?;
        }

        Ok(())
    }

    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        let mut columns = Vec::with_capacity(self.funcs.len());

        for (idx, func) in self.funcs.iter().enumerate() {
            let place = self.places[idx];

            let mut array_builder = MutableStringColumn::with_capacity(4);
            func.serialize(place, array_builder.values_mut())?;
            array_builder.commit_row();
            columns.push(array_builder.to_column());
        }

        // TODO: create with temp schema
        self.drop_states();
        Ok(vec![DataBlock::create(self.schema.clone(), columns)])
    }
}

impl<const FINAL: bool> Drop for SingleStateAggregator<FINAL> {
    fn drop(&mut self) {
        self.drop_states();
    }
}
