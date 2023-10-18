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
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use serde::Deserialize;
use serde::Serialize;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function::AggregateFunctionRef;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::deserialize_state;
use super::serialize_state;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;

#[derive(Serialize, Deserialize)]
struct AggregateRetentionState {
    pub events: u32,
}

impl AggregateRetentionState {
    #[inline(always)]
    fn add(&mut self, event: u8) {
        self.events |= 1 << event;
    }

    fn merge(&mut self, other: &Self) {
        self.events |= other.events;
    }
}

#[derive(Clone)]
pub struct AggregateRetentionFunction {
    display_name: String,
    events_size: u8,
}

impl AggregateFunction for AggregateRetentionFunction {
    fn name(&self) -> &str {
        "AggregateRetentionFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Number(
            NumberDataType::UInt8,
        ))))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateRetentionState { events: 0 });
    }

    fn state_layout(&self) -> std::alloc::Layout {
        Layout::new::<AggregateRetentionState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        _validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let new_columns = columns
            .iter()
            .map(|col| BooleanType::try_downcast_column(col).unwrap())
            .collect::<Vec<_>>();
        for i in 0..input_rows {
            for j in 0..self.events_size {
                if new_columns[j as usize].get_bit(i) {
                    state.add(j);
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let new_columns = columns
            .iter()
            .map(|col| BooleanType::try_downcast_column(col).unwrap())
            .collect::<Vec<_>>();
        for (row, place) in places.iter().enumerate() {
            let place = place.next(offset);
            let state = place.get::<AggregateRetentionState>();
            for j in 0..self.events_size {
                if new_columns[j as usize].get_bit(row) {
                    state.add(j);
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let new_columns = columns
            .iter()
            .map(|col| BooleanType::try_downcast_column(col).unwrap())
            .collect::<Vec<_>>();
        for j in 0..self.events_size {
            if new_columns[j as usize].get_bit(row) {
                state.add(j);
            }
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let rhs: AggregateRetentionState = deserialize_state(reader)?;
        state.merge(&rhs);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let other = rhs.get::<AggregateRetentionState>();
        state.merge(other);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let builder = builder.as_array_mut().unwrap();
        let inner = builder
            .builder
            .as_number_mut()
            .unwrap()
            .as_u_int8_mut()
            .unwrap();

        inner.reserve(self.events_size as usize);
        if state.events & 1 == 1 {
            inner.push(1u8);
            for i in 1..self.events_size {
                if state.events & (1 << i) != 0 {
                    inner.push(1u8);
                } else {
                    inner.push(0u8);
                }
            }
        } else {
            for _ in 0..self.events_size {
                inner.push(0u8);
            }
        }
        builder.offsets.push(builder.builder.len() as u64);
        Ok(())
    }
}

impl fmt::Display for AggregateRetentionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateRetentionFunction {
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            events_size: arguments.len() as u8,
        }))
    }
}

pub fn try_create_aggregate_retention_function(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_variadic_arguments(display_name, arguments.len(), (1, 32))?;

    for argument in arguments.iter() {
        if !argument.is_boolean() {
            return Err(ErrorCode::BadArguments(
                "The arguments of AggregateRetention should be an expression which returns a Boolean result",
            ));
        }
    }

    AggregateRetentionFunction::try_create(display_name, arguments)
}

pub fn aggregate_retention_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_retention_function))
}
