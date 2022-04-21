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
use std::fmt;
use std::sync::Arc;

use bytes::BytesMut;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use serde_json::json;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function::AggregateFunctionRef;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;

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

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, &self.events)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.events = deserialize_from_slice(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateRetentionFunction {
    display_name: String,
    events_size: u8,
    _arguments: Vec<DataField>,
}

impl AggregateFunction for AggregateRetentionFunction {
    fn name(&self) -> &str {
        "AggregateRetentionFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(VariantValue::to_data_type())
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
        columns: &[common_datavalues::ColumnRef],
        _validity: Option<&common_arrow::arrow::bitmap::Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let new_columns: Vec<&BooleanColumn> = columns
            .iter()
            .map(|column| Series::check_get(column).unwrap())
            .collect();
        for i in 0..input_rows {
            for j in 0..self.events_size {
                if new_columns[j as usize].get_data(i) {
                    state.add(j);
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(
        &self,
        place: StateAddr,
        columns: &[common_datavalues::ColumnRef],
        row: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let new_columns: Vec<&BooleanColumn> = columns
            .iter()
            .map(|column| Series::check_get(column).unwrap())
            .collect();
        for j in 0..self.events_size {
            if new_columns[j as usize].get_data(row) {
                state.add(j);
            }
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<AggregateRetentionState>();
        let state = place.get::<AggregateRetentionState>();
        state.merge(rhs);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(
        &self,
        place: StateAddr,
        array: &mut dyn common_datavalues::MutableColumn,
    ) -> Result<()> {
        let state = place.get::<AggregateRetentionState>();
        let builder: &mut MutableObjectColumn<VariantValue> =
            Series::check_get_mutable_column(array)?;
        let mut vec: Vec<u8> = vec![0; self.events_size as usize];
        if state.events & 1 == 1 {
            vec[0] = 1;
            for i in 1..self.events_size {
                if state.events & (1 << i) != 0 {
                    vec[i as usize] = 1;
                }
            }
        }
        builder.append_value(VariantValue::from(json!(vec)));
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[common_datavalues::ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let new_columns: Vec<&BooleanColumn> = columns
            .iter()
            .map(|column| Series::check_get(column).unwrap())
            .collect();
        for (row, place) in places.iter().enumerate() {
            let place = place.next(offset);
            let state = place.get::<AggregateRetentionState>();
            for j in 0..self.events_size {
                if new_columns[j as usize].get_data(row) {
                    state.add(j);
                }
            }
        }
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
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            events_size: arguments.len() as u8,
            _arguments: arguments,
        }))
    }
}

pub fn try_create_aggregate_retention_function(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<AggregateFunctionRef> {
    assert_variadic_arguments(display_name, arguments.len(), (1, 32))?;

    for argument in arguments.iter() {
        let data_type = argument.data_type();
        if data_type.data_type_id() != TypeID::Boolean {
            return Err(ErrorCode::BadArguments(
                "The arguments of AggregateRetention should be an expression which returns a Boolean result"
            ));
        }
    }

    AggregateRetentionFunction::try_create(display_name, arguments)
}

pub fn aggregate_retention_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_retention_function))
}
