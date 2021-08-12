// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::alloc::Layout;
use std::fmt;

use bytes::BytesMut;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;

use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;
use crate::aggregates::AggregateFunction;

pub struct AggregateCountState {
    count: u64,
}

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

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateCountState { count: 0 });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateCountState>()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], input_rows: usize) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        if self.arguments.is_empty() {
            state.count += input_rows as u64;
        } else {
            state.count += (input_rows - arrays[0].null_count()) as u64;
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        if self.arguments.is_empty() {
            for place in places.iter() {
                let state = place.get::<AggregateCountState>();
                state.count += 1;
            }
            return Ok(());
        }

        let array = arrays[0].get_array_ref();
        let validity = array.validity();
        for (row, place) in places.iter().enumerate() {
            let state = place.get::<AggregateCountState>();
            if let Some(v) = validity {
                state.count += v.get_bit(row) as u64;
            }
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        writer.write_uvarint(state.count)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        reader.read_to_uvarint(&mut state.count)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let rhs = rhs.get::<AggregateCountState>();
        state.count += rhs.count;

        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = place.get::<AggregateCountState>();
        Ok(state.count.into())
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
