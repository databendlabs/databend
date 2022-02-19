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
use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Sub;
use std::sync::Arc;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::AggregateFunctionRef;
use super::AggregateNullVariadicAdaptor;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_params;
use crate::aggregates::assert_variadic_arguments;
use crate::aggregates::AggregateFunction;
use crate::with_match_date_date_time_types;
use crate::with_match_unsigned_numeric_types;

#[derive(Serialize, Deserialize)]
struct AggregateWindowFunnelState<T> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub events_list: Vec<(T, u8)>,
    pub sorted: bool,
}

impl<T> AggregateWindowFunnelState<T>
where T: Ord
        + Sub<Output = T>
        + AsPrimitive<u64>
        + Serialize
        + DeserializeOwned
        + Clone
        + Send
        + Sync
        + 'static
{
    pub fn new() -> Self {
        Self {
            events_list: Vec::new(),
            sorted: true,
        }
    }
    #[inline(always)]
    fn add(&mut self, timestamp: T, event: u8) {
        if self.sorted && !self.events_list.is_empty() {
            let last = self.events_list.last().unwrap();
            if last.0 == timestamp {
                self.sorted = last.1 <= event;
            } else {
                self.sorted = last.0 <= timestamp;
            }
        }
        self.events_list.push((timestamp, event));
    }

    #[inline(always)]
    fn merge(&mut self, other: &mut Self) {
        if other.events_list.is_empty() {
            return;
        }
        let l1 = self.events_list.len();
        let l2 = other.events_list.len();

        self.sort();
        other.sort();
        let mut merged = Vec::with_capacity(self.events_list.len() + other.events_list.len());
        let cmp = |a: &(T, u8), b: &(T, u8)| {
            let ord = a.0.cmp(&b.0);
            if ord == Ordering::Equal {
                a.1.cmp(&b.1)
            } else {
                ord
            }
        };

        {
            let mut i = 0;
            let mut j = 0;
            let mut k = 0;
            while i < l1 && j < l2 {
                if cmp(&self.events_list[i], &other.events_list[j]) == Ordering::Less {
                    merged.push(self.events_list[i]);
                    k += 1;
                    i += 1;
                } else {
                    merged.push(other.events_list[j]);
                    k += 1;
                    j += 1;
                }
            }

            unsafe {
                merged.set_len(self.events_list.len() + other.events_list.len());
            }

            if i < l1 {
                merged[k..].copy_from_slice(&self.events_list[i..]);
            }
            if j < l2 {
                merged[k..].copy_from_slice(&other.events_list[j..]);
            }
        }
        self.events_list = merged;
    }

    #[inline(always)]
    fn sort(&mut self) {
        let cmp = |a: &(T, u8), b: &(T, u8)| {
            let ord = a.0.cmp(&b.0);
            if ord == Ordering::Equal {
                a.1.cmp(&b.1)
            } else {
                ord
            }
        };
        if !self.sorted {
            self.events_list.sort_by(cmp);
        }
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, self)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        *self = deserialize_from_slice(reader)?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateWindowFunnelFunction<T> {
    display_name: String,
    _arguments: Vec<DataField>,
    event_size: usize,
    window: u64,
    t: PhantomData<T>,
}

impl<T> AggregateFunction for AggregateWindowFunnelFunction<T>
where
    T: PrimitiveType,
    T: Ord + Sub<Output = T> + AsPrimitive<u64> + Clone + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        "AggregateWindowFunnelFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(u8::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(AggregateWindowFunnelState::<T>::new);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateWindowFunnelState<T>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let mut dcolumns = Vec::with_capacity(self.event_size);
        for i in 0..self.event_size {
            let dcolumn: &BooleanColumn = unsafe { Series::static_cast(&columns[i + 1]) };
            dcolumns.push(dcolumn.values());
        }

        let tcolumn: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        let state = place.get::<AggregateWindowFunnelState<T>>();

        match validity {
            Some(bitmap) => {
                for ((row, timestamp), valid) in tcolumn.iter().enumerate().zip(bitmap.iter()) {
                    if valid {
                        for (i, filter) in dcolumns.iter().enumerate() {
                            if filter.get_bit(row) {
                                state.add(*timestamp, (i + 1) as u8);
                            }
                        }
                    }
                }
            }
            None => {
                for (row, timestamp) in tcolumn.iter().enumerate() {
                    for (i, filter) in dcolumns.iter().enumerate() {
                        if filter.get_bit(row) {
                            state.add(*timestamp, (i + 1) as u8);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let mut dcolumns = Vec::with_capacity(self.event_size);
        for i in 0..self.event_size {
            let dcolumn: &BooleanColumn = unsafe { Series::static_cast(&columns[i + 1]) };
            dcolumns.push(dcolumn.values());
        }

        let tcolumn: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };

        for ((row, timestamp), place) in tcolumn.iter().enumerate().zip(places.iter()) {
            let state = (place.next(offset)).get::<AggregateWindowFunnelState<T>>();
            for (i, filter) in dcolumns.iter().enumerate() {
                if filter.get_bit(row) {
                    state.add(*timestamp, (i + 1) as u8);
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let tcolumn: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        let timestamp = unsafe { tcolumn.value_unchecked(row) };

        let state = place.get::<AggregateWindowFunnelState<T>>();
        for i in 0..self.event_size {
            let dcolumn: &BooleanColumn = unsafe { Series::static_cast(&columns[i + 1]) };
            if dcolumn.values().get_bit(row) {
                state.add(timestamp, (i + 1) as u8);
            }
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateWindowFunnelState<T>>();
        AggregateWindowFunnelState::<T>::serialize(state, writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateWindowFunnelState<T>>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<AggregateWindowFunnelState<T>>();
        let state = place.get::<AggregateWindowFunnelState<T>>();

        state.merge(rhs);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, column: &mut dyn MutableColumn) -> Result<()> {
        let column: &mut MutablePrimitiveColumn<u8> = Series::check_get_mutable_column(column)?;
        let result = self.get_event_level(place);
        column.append_value(result);
        Ok(())
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: AggregateFunctionRef,
        _params: Vec<DataValue>,
        _arguments: Vec<DataField>,
    ) -> Result<Option<AggregateFunctionRef>> {
        Ok(Some(AggregateNullVariadicAdaptor::<false, false>::create(
            Arc::new(self.clone()),
        )))
    }
}

impl<T> fmt::Display for AggregateWindowFunnelFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateWindowFunnelFunction<T>
where
    T: PrimitiveType,
    T: Ord + Sub<Output = T> + AsPrimitive<u64> + Clone + Send + Sync + 'static,
{
    pub fn try_create(
        display_name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        let event_size = arguments.len() - 1;
        let window = params[0].as_u64()?;
        Ok(Arc::new(Self {
            display_name: display_name.to_owned(),
            _arguments: arguments,
            event_size,
            window,
            t: PhantomData,
        }))
    }

    /// Loop through the entire events_list, update the event timestamp value
    /// The level path must be 1---2---3---...---check_events_size, find the max event level that satisfied the path in the sliding window.
    /// If found, returns the max event level, else return 0.
    /// The Algorithm complexity is O(n).
    fn get_event_level(&self, place: StateAddr) -> u8 {
        let state = place.get::<AggregateWindowFunnelState<T>>();
        if state.events_list.is_empty() {
            return 0;
        }
        if self.event_size == 1 {
            return 1;
        }

        state.sort();

        let mut events_timestamp: Vec<Option<T>> = Vec::with_capacity(self.event_size);
        for _i in 0..self.event_size {
            events_timestamp.push(None);
        }
        for (timestamp, event) in state.events_list.iter() {
            let event_idx = (event - 1) as usize;

            if event_idx == 0 {
                events_timestamp[event_idx] = Some(*timestamp);
            } else if let Some(v) = events_timestamp[event_idx - 1] {
                // we already sort the events_list
                let window: u64 = timestamp.sub(v).as_();
                if window <= self.window {
                    events_timestamp[event_idx] = events_timestamp[event_idx - 1];
                }
            }
        }

        for i in (0..self.event_size).rev() {
            if events_timestamp[i].is_some() {
                return i as u8 + 1;
            }
        }

        0
    }
}

macro_rules! creator {
    ($T: ident,  $display_name: expr, $params: expr, $arguments: expr) => {
        return AggregateWindowFunnelFunction::<$T>::try_create($display_name, $params, $arguments);
    };
}

pub fn try_create_aggregate_window_funnel_function(
    display_name: &str,
    params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<AggregateFunctionRef> {
    assert_unary_params(display_name, params.len())?;
    assert_variadic_arguments(display_name, arguments.len(), (1, 32))?;

    for (idx, arg) in arguments[1..].iter().enumerate() {
        if arg.data_type().data_type_id() != TypeID::Boolean {
            return Err(ErrorCode::BadDataValueType(format!(
                "Illegal type of the argument {:?} in AggregateWindowFunnelFunction, must be boolean, got: {:?}",
                 idx + 1, arg.data_type()
            )));
        }
    }

    let data_type = arguments[0].data_type();
    with_match_date_date_time_types! {creator, data_type.data_type_id(), display_name, params, arguments}
    with_match_unsigned_numeric_types! {creator, data_type.data_type_id(), display_name, params, arguments}

    Err(ErrorCode::BadDataValueType(format!(
        "AggregateWindowFunnelFunction does not support type '{:?}'",
        data_type
    )))
}

pub fn aggregate_window_funnel_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_window_funnel_function))
}
