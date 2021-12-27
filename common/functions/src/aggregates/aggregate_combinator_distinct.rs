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

use std::alloc::Layout;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutablePrimitiveArray;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use serde::Deserialize;
use serde::Serialize;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionCreator;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_function_factory::CombinatorDescription;
use crate::aggregates::aggregator_common::assert_variadic_arguments;
use crate::aggregates::AggregateCountFunction;
use crate::aggregates::AggregateFunction;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
}

impl AggregateDistinctState {
    pub fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        let writer = BufMut::writer(writer);
        bincode::serialize_into(writer, &self.set)?;
        Ok(())
    }

    pub fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.set = bincode::deserialize_from(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateDistinctCombinator {
    name: String,

    nested_name: String,
    arguments: Vec<DataField>,
    nested: Arc<dyn AggregateFunction>,
}

impl AggregateDistinctCombinator {
    pub fn try_create_uniq(
        nested_name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let creator: AggregateFunctionCreator = Box::new(AggregateCountFunction::try_create);
        AggregateDistinctCombinator::try_create(nested_name, params, arguments, &creator)
    }

    pub fn uniq_desc() -> AggregateFunctionDescription {
        AggregateFunctionDescription::creator(Box::new(Self::try_create_uniq))
    }

    pub fn try_create(
        nested_name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let name = format!("DistinctCombinator({})", nested_name);
        assert_variadic_arguments(&name, arguments.len(), (1, 32))?;

        let nested_arguments = match nested_name {
            "count" | "uniq" => vec![],
            _ => arguments.clone(),
        };

        let nested = nested_creator(nested_name, params, nested_arguments)?;
        Ok(Arc::new(AggregateDistinctCombinator {
            nested_name: nested_name.to_owned(),
            arguments,
            nested,
            name,
        }))
    }

    pub fn combinator_desc() -> CombinatorDescription {
        CombinatorDescription::creator(Box::new(Self::try_create))
    }
}

impl AggregateFunction for AggregateDistinctCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.nested.nullable(input_schema)
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateDistinctState {
            set: HashSet::new(),
        });

        let layout = Layout::new::<AggregateDistinctState>();
        let netest_place = place.next(layout.size());
        self.nested.init_state(netest_place);
    }

    fn state_layout(&self) -> Layout {
        let layout = Layout::new::<AggregateDistinctState>();
        let netesed = self.nested.state_layout();
        Layout::from_size_align(layout.size() + netesed.size(), layout.align()).unwrap()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], input_rows: usize) -> Result<()> {
        for row in 0..input_rows {
            let values = arrays
                .iter()
                .map(|s| s.try_get(row))
                .collect::<Result<Vec<_>>>()?;

            if !values.iter().any(|c| c.is_null()) {
                let state = place.get::<AggregateDistinctState>();
                state.set.insert(DataGroupValues(
                    values
                        .iter()
                        .map(DataGroupValue::try_from)
                        .collect::<Result<Vec<_>>>()?,
                ));
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        for (row, place) in places.iter().enumerate() {
            let values = arrays
                .iter()
                .map(|s| s.try_get(row))
                .collect::<Result<Vec<_>>>()?;

            if !values.iter().any(|c| c.is_null()) {
                let place = place.next(offset);
                let state = place.get::<AggregateDistinctState>();
                state.set.insert(DataGroupValues(
                    values
                        .iter()
                        .map(DataGroupValue::try_from)
                        .collect::<Result<Vec<_>>>()?,
                ));
            }
        }

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateDistinctState>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateDistinctState>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateDistinctState>();
        let rhs = rhs.get::<AggregateDistinctState>();

        state.set.extend(rhs.set.clone());
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableArray) -> Result<()> {
        let state = place.get::<AggregateDistinctState>();

        let layout = Layout::new::<AggregateDistinctState>();
        let netest_place = place.next(layout.size());

        // faster path for count
        if self.nested.name() == "AggregateFunctionCount" {
            let mut array = array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<u64>>()
                .ok_or_else(|| {
                    ErrorCode::UnexpectedError(
                        "error occured when downcast MutableArray".to_string(),
                    )
                })?;
            array.push(Some(state.set.len() as u64));
            Ok(())
        } else {
            if state.set.is_empty() {
                return self.nested.merge_result(netest_place, array);
            }
            let mut results = Vec::with_capacity(state.set.len());

            state.set.iter().for_each(|group_values| {
                let mut v = Vec::with_capacity(group_values.0.len());
                group_values.0.iter().for_each(|group_value| {
                    v.push(DataValue::from(group_value));
                });

                results.push(v);
            });

            let results = (0..self.arguments.len())
                .map(|i| {
                    results
                        .iter()
                        .map(|inner| inner[i].clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let arrays = results
                .iter()
                .enumerate()
                .map(|(i, v)| DataValue::try_into_data_array(v, self.arguments[i].data_type()))
                .collect::<Result<Vec<_>>>()?;

            self.nested
                .accumulate(netest_place, &arrays, state.set.len())?;
            // merge_result
            self.nested.merge_result(netest_place, array)
        }
    }
}

impl fmt::Display for AggregateDistinctCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}
