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

use std::borrow::BorrowMut;
use std::sync::Arc;

use bumpalo::Bump;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_expression::Value;
use common_functions_v2::aggregates::AggregateFunctionRef;
use common_functions_v2::aggregates::StateAddr;

use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;

pub type FinalSingleStateAggregator = SingleStateAggregator<true>;
pub type PartialSingleStateAggregator = SingleStateAggregator<false>;

/// SELECT COUNT | SUM FROM table;
pub struct SingleStateAggregator<const FINAL: bool> {
    funcs: Vec<AggregateFunctionRef>,
    arg_indices: Vec<Vec<usize>>,
    schema: DataSchemaRef,
    _arena: Bump,
    places: Vec<StateAddr>,
    // used for deserialization only, so we can reuse it during the loop
    temp_places: Vec<StateAddr>,
    states_dropped: bool,
}

impl<const FINAL: bool> SingleStateAggregator<FINAL> {
    pub fn try_create(params: &Arc<AggregatorParams>) -> Result<Self> {
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
        let temp_places = get_places();
        Ok(Self {
            _arena: arena,
            places,
            funcs: params.aggregate_functions.clone(),
            arg_indices: params.aggregate_functions_arguments.clone(),
            schema: params.output_schema.clone(),
            temp_places,
            states_dropped: false,
        })
    }

    fn drop_states(&mut self) {
        if !self.states_dropped {
            for (place, func) in self.places.iter().zip(self.funcs.iter()) {
                if func.need_manual_drop_state() {
                    unsafe { func.drop_state(*place) }
                }
            }

            for (place, func) in self.temp_places.iter().zip(self.funcs.iter()) {
                if func.need_manual_drop_state() {
                    unsafe { func.drop_state(*place) }
                }
            }

            self.states_dropped = true;
        }
    }
}

impl Aggregator for SingleStateAggregator<true> {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn consume(&mut self, chunk: Chunk) -> Result<()> {
        let chunk = chunk.convert_to_full();
        for (index, func) in self.funcs.iter().enumerate() {
            let place = self.places[index];

            let binary_array = chunk.column(index).0.as_column().unwrap();
            let binary_array = binary_array.as_string().ok_or(ErrorCode::IllegalDataType(
                "binary array should be string type",
            ))?;

            let mut data = unsafe { binary_array.index_unchecked(0) };

            let temp_addr = self.temp_places[index];
            func.deserialize(temp_addr, &mut data)?;
            func.merge(place, temp_addr)?;
        }

        Ok(())
    }

    fn generate(&mut self) -> Result<Vec<Chunk>> {
        let mut aggr_values = {
            let mut builders = vec![];
            for func in &self.funcs {
                let data_type = func.return_type()?;
                builders.push((ColumnBuilder::with_capacity(&data_type, 1024), data_type));
            }
            builders
        };

        for (index, func) in self.funcs.iter().enumerate() {
            let place = self.places[index];
            let (builder, _) = aggr_values[index].borrow_mut();
            func.merge_result(place, builder)?;
        }

        let mut num_rows = 0;
        let mut columns: Vec<(Value<AnyType>, DataType)> = Vec::with_capacity(self.funcs.len());
        for (builder, ty) in aggr_values {
            num_rows = builder.len();
            let col = builder.build();
            columns.push((Value::Column(col), ty));
        }

        Ok(vec![Chunk::new(columns, num_rows)])
    }
}

impl Aggregator for SingleStateAggregator<false> {
    const NAME: &'static str = "AggregatorPartialTransform";

    fn consume(&mut self, chunk: Chunk) -> Result<()> {
        let chunk = chunk.convert_to_full();
        let rows = chunk.num_rows();
        for (idx, func) in self.funcs.iter().enumerate() {
            let mut arg_columns = vec![];
            for index in self.arg_indices[idx].iter() {
                arg_columns.push(chunk.column(*index).clone());
            }
            let place = self.places[idx];
            func.accumulate(place, &arg_columns, None, rows)?;
        }

        Ok(())
    }

    fn generate(&mut self) -> Result<Vec<Chunk>> {
        let mut columns = Vec::with_capacity(self.funcs.len());

        for (idx, func) in self.funcs.iter().enumerate() {
            let place = self.places[idx];

            let mut data = Vec::with_capacity(4);
            func.serialize(place, &mut data)?;
            columns.push((Value::Scalar(Scalar::String(data)), DataType::String));
        }

        // TODO: create with temp schema
        self.drop_states();
        Ok(vec![Chunk::new(columns, 1)])
    }
}

impl<const FINAL: bool> Drop for SingleStateAggregator<FINAL> {
    fn drop(&mut self) {
        self.drop_states();
    }
}
