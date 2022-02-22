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

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionCreator;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_function_factory::CombinatorDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;
use crate::aggregates::AggregateCountFunction;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
}

impl AggregateDistinctState {
    pub fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, &self.set)
    }

    pub fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.set = deserialize_from_slice(reader)?;

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
        let properties = super::aggregate_function_factory::AggregateFunctionProperties {
            returns_default_when_only_null: true,
        };
        AggregateFunctionDescription::creator_with_properties(
            Box::new(Self::try_create_uniq),
            properties,
        )
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

    fn return_type(&self) -> Result<DataTypePtr> {
        self.nested.return_type()
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

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        for row in 0..input_rows {
            let values = columns.iter().map(|s| s.get(row)).collect::<Vec<_>>();

            match validity {
                Some(v) => {
                    if v.get_bit(row) {
                        let data_values = DataGroupValues(
                            values
                                .iter()
                                .map(DataGroupValue::try_from)
                                .collect::<Result<Vec<_>>>()?,
                        );
                        let state = place.get::<AggregateDistinctState>();
                        state.set.insert(data_values);
                    }
                }
                None => {
                    let data_values = DataGroupValues(
                        values
                            .iter()
                            .map(DataGroupValue::try_from)
                            .collect::<Result<Vec<_>>>()?,
                    );
                    let state = place.get::<AggregateDistinctState>();
                    state.set.insert(data_values);
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let values = columns.iter().map(|s| s.get(row)).collect::<Vec<_>>();
        let state = place.get::<AggregateDistinctState>();
        state.set.insert(DataGroupValues(
            values
                .iter()
                .map(DataGroupValue::try_from)
                .collect::<Result<Vec<_>>>()?,
        ));

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
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<AggregateDistinctState>();

        let layout = Layout::new::<AggregateDistinctState>();
        let netest_place = place.next(layout.size());

        // faster path for count
        if self.nested.name() == "AggregateFunctionCount" {
            let mut builder: &mut MutablePrimitiveColumn<u64> =
                Series::check_get_mutable_column(array)?;
            builder.append_value(state.set.len() as u64);
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

            let columns = results
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let data_type = self.arguments[i].data_type();
                    data_type.create_column(v)
                })
                .collect::<Result<Vec<_>>>()?;

            self.nested
                .accumulate(netest_place, &columns, None, state.set.len())?;
            // merge_result
            self.nested.merge_result(netest_place, array)
        }
    }
}

impl fmt::Display for AggregateDistinctCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.nested_name.as_str() {
            "uniq" => write!(f, "uniq"),
            _ => write!(f, "{}distinct", self.nested_name),
        }
    }
}
