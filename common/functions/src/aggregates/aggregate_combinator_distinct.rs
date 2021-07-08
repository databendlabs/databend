// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;
use std::io::Cursor;

use bytes::Buf;
use common_datavalues::prelude::*;
use common_exception::Result;

use super::GetState;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::FactoryFunc;
use crate::aggregates::aggregator_common::assert_variadic_arguments;
use crate::aggregates::AggregateCountFunction;
use crate::aggregates::AggregateFunction;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
}

impl AggregateDistinctState {
    pub fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        let mut vs = Vec::with_capacity(self.set.len());
        for entry in self.set.iter() {
            let array = entry
                .0
                .iter()
                .map(|v| DataValue::from(v))
                .collect::<Vec<_>>();
            vs.push(array);
        }

        serde_json::to_writer(writer, &vs)?;
        Ok(())
    }

    pub fn deserialize(&mut self, reader: &[u8]) -> Result<()> {
        self.set.clear();
        let reader = Cursor::new(reader).reader();
        let vs: Vec<Vec<DataValue>> = serde_json::from_reader(reader)?;

        for array in vs.iter() {
            let v = array
                .iter()
                .map(|value| DataGroupValue::try_from(value))
                .collect::<Result<Vec<_>>>()?;

            self.set.insert(DataGroupValues(v));
        }
        Ok(())
    }
}

impl<'a> GetState<'a, AggregateDistinctState> for AggregateDistinctState {}

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
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        AggregateDistinctCombinator::try_create(
            nested_name,
            arguments,
            AggregateCountFunction::try_create,
        )
    }

    pub fn try_create(
        nested_name: &str,
        arguments: Vec<DataField>,
        nested_creator: FactoryFunc,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let name = format!("DistinctCombinator({})", nested_name);
        assert_variadic_arguments(&name, arguments.len(), (1, 32))?;

        let nested_arguments = match nested_name {
            "count" | "uniq" => vec![],
            _ => arguments.clone(),
        };

        let nested = nested_creator(nested_name, nested_arguments)?;
        Ok(Arc::new(AggregateDistinctCombinator {
            nested_name: nested_name.to_owned(),
            arguments,
            nested,
            name,
        }))
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        self.nested.allocate_state(arena);
        let state = arena.alloc(AggregateDistinctState {
            set: HashSet::new(),
        });

        (state as *mut AggregateDistinctState) as StateAddr
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, columns: &[DataColumn]) -> Result<()> {
        let state = AggregateDistinctState::get(place);

        let values = columns
            .iter()
            .map(|c| c.try_get(row))
            .collect::<Result<Vec<_>>>()?;
        if !values.iter().any(|c| c.is_null()) {
            state.set.insert(DataGroupValues(
                values
                    .iter()
                    .map(DataGroupValue::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ));
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = AggregateDistinctState::get(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &[u8]) -> Result<()> {
        let state = AggregateDistinctState::get(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = AggregateDistinctState::get(place);
        let rhs = AggregateDistinctState::get(rhs);

        state.set.extend(rhs.set.clone());
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateDistinctState::get(place);

        // faster path for count
        if self
            .nested
            .as_any()
            .downcast_ref::<AggregateCountFunction>()
            .is_some()
        {
            Ok(DataValue::UInt64(Some(state.set.len() as u64)))
        } else {
            let offsize = std::mem::size_of::<AggregateDistinctState>();
            let nested_place = (place as usize + offsize) as *mut u8;
            if state.set.is_empty() {
                return self.nested.merge_result(nested_place);
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
                .map(|v| DataValue::try_into_data_array(v).map(|s| DataColumn::Array(s)))
                .collect::<Result<Vec<_>>>()?;

            self.nested
                .accumulate(nested_place, &columns, state.set.len())?;
            // merge_result
            self.nested.merge_result(nested_place)
        }
    }
}

impl fmt::Display for AggregateDistinctCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}
