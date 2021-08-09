// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use bytes::BytesMut;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;

use super::StateAddr;
use crate::aggregates::aggregate_function_state::GetState;
use crate::aggregates::aggregator_common::assert_variadic_arguments;
use crate::aggregates::AggregateFunction;

pub struct AggregateCountState {
    count: u64,
}

impl<'a> GetState<'a, AggregateCountState> for AggregateCountState {}

#[derive(Clone)]
pub struct AggregateCountFunction {
    display_name: String,
    arguments: Vec<DataField>,
}

impl AggregateCountFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Arc::new(AggregateCountFunction {
            display_name: display_name.to_string(),
            arguments,
        }))
    }
}

impl AggregateFunction for AggregateCountFunction {
    fn name(&self) -> &str {
        "AggregateCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateCountState { count: 0 });
        (state as *mut AggregateCountState) as StateAddr
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], input_rows: usize) -> Result<()> {
        let state = AggregateCountState::get(place);
        if self.arguments.is_empty() {
            state.count += input_rows as u64;
        } else {
            state.count += (input_rows - arrays[0].null_count()) as u64;
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, arrays: &[Series]) -> Result<()> {
        let state = AggregateCountState::get(place);
        if self.arguments.is_empty() {
            state.count += 1;
        } else {
            state.count += arrays[0].is_null(row) as u64;
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = AggregateCountState::get(place);
        writer.write_uvarint(state.count)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = AggregateCountState::get(place);
        reader.read_to_uvarint(&mut state.count)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = AggregateCountState::get(place);
        let rhs = AggregateCountState::get(rhs);
        state.count += rhs.count;

        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateCountState::get(place);
        Ok(state.count.into())
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
