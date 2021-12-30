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
use std::fmt;
use std::sync::Arc;

use bytes::BytesMut;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
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
        _params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Arc::new(AggregateCountFunction {
            display_name: display_name.to_string(),
            arguments,
        }))
    }

    pub fn desc() -> AggregateFunctionDescription {
        AggregateFunctionDescription::creator(Box::new(Self::try_create))
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
        offset: usize,
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        if self.arguments.is_empty() {
            for place in places.iter() {
                let place = place.next(offset);
                let state = place.get::<AggregateCountState>();
                state.count += 1;
            }
            return Ok(());
        }

        match arrays[0].get_array_ref().validity() {
            None => {
                for place in places.iter() {
                    let place = place.next(offset);
                    let state = place.get::<AggregateCountState>();
                    state.count += 1;
                }
            }
            Some(nullable_marks) => {
                for (row, place) in places.iter().enumerate() {
                    let place = place.next(offset);
                    let state = place.get::<AggregateCountState>();
                    state.count += nullable_marks.get_bit(row) as u64;
                }
            }
        }

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        serialize_into_buf(writer, &state.count)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        state.count = deserialize_from_slice(reader)?;

        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let rhs = rhs.get::<AggregateCountState>();
        state.count += rhs.count;

        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableArrayBuilder) -> Result<()> {
        let mut array = array
            .as_mut_any()
            .downcast_mut::<MutablePrimitiveArrayBuilder<u64>>()
            .ok_or_else(|| {
                ErrorCode::UnexpectedError("error occured when downcast MutableArray".to_string())
            })?;
        let state = place.get::<AggregateCountState>();
        array.push(state.count);
        Ok(())
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
