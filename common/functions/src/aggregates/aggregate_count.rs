// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::convert::TryInto;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateCountState { count: 0 });
        (state as *mut AggregateCountState) as StateAddr
    }

    fn accumulate(
        &self,
        place: StateAddr,
        _columns: &[DataColumn],
        input_rows: usize,
    ) -> Result<()> {
        let state = AggregateCountState::get(place);
        state.count += input_rows as u64;

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, _row: usize, _columns: &[DataColumn]) -> Result<()> {
        let state = AggregateCountState::get(place);
        state.count += 1;
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = AggregateCountState::get(place);
        let bs = state.count.to_be_bytes();
        writer.extend_from_slice(&bs);
        Ok(())
    }

    fn deserialize(&self, place: StateAddr, value: &[u8]) -> Result<()> {
        let state = AggregateCountState::get(place);
        state.count = u64::from_be_bytes(value.try_into().unwrap());
        Ok(())
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
