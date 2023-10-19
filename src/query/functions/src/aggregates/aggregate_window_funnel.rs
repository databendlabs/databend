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
use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Sub;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::number::Number;
use common_expression::types::number::UInt8Type;
use common_expression::types::ArgType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::with_integer_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use num_traits::AsPrimitive;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use super::AggregateFunctionRef;
use super::AggregateNullVariadicAdaptor;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_params;
use crate::aggregates::assert_variadic_arguments;
use crate::aggregates::AggregateFunction;
use crate::BUILTIN_FUNCTIONS;

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
            while i < l1 && j < l2 {
                if cmp(&self.events_list[i], &other.events_list[j]) == Ordering::Less {
                    merged.push(self.events_list[i]);
                    i += 1;
                } else {
                    merged.push(other.events_list[j]);
                    j += 1;
                }
            }

            if i < l1 {
                merged.extend(self.events_list[i..].iter());
            }
            if j < l2 {
                merged.extend(other.events_list[j..].iter());
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
}

#[derive(Clone)]
pub struct AggregateWindowFunnelFunction<T> {
    display_name: String,
    _arguments: Vec<DataType>,
    event_size: usize,
    window: u64,
    t: PhantomData<T>,
}

impl<T> AggregateFunction for AggregateWindowFunnelFunction<T>
where
    T: ArgType + Send + Sync,
    T::Scalar: Number
        + Ord
        + Sub<Output = T::Scalar>
        + AsPrimitive<u64>
        + Clone
        + Serialize
        + DeserializeOwned
        + 'static,
{
    fn name(&self) -> &str {
        "AggregateWindowFunnelFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::UInt8))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(AggregateWindowFunnelState::<T::Scalar>::new);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateWindowFunnelState<T::Scalar>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let mut dcolumns = Vec::with_capacity(self.event_size);
        for i in 0..self.event_size {
            let dcolumn = BooleanType::try_downcast_column(&columns[i + 1]).unwrap();

            dcolumns.push(dcolumn);
        }

        let tcolumn = T::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();

        match validity {
            Some(bitmap) => {
                for ((row, timestamp), valid) in
                    T::iter_column(&tcolumn).enumerate().zip(bitmap.iter())
                {
                    if valid {
                        let timestamp = T::to_owned_scalar(timestamp);
                        for (i, filter) in dcolumns.iter().enumerate() {
                            if filter.get_bit(row) {
                                state.add(timestamp, (i + 1) as u8);
                            }
                        }
                    }
                }
            }
            None => {
                for (row, timestamp) in T::iter_column(&tcolumn).enumerate() {
                    let timestamp = T::to_owned_scalar(timestamp);
                    for (i, filter) in dcolumns.iter().enumerate() {
                        if filter.get_bit(row) {
                            state.add(timestamp, (i + 1) as u8);
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
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let mut dcolumns = Vec::with_capacity(self.event_size);
        for i in 0..self.event_size {
            let dcolumn = BooleanType::try_downcast_column(&columns[i + 1]).unwrap();
            dcolumns.push(dcolumn);
        }

        let tcolumn = T::try_downcast_column(&columns[0]).unwrap();

        for ((row, timestamp), place) in T::iter_column(&tcolumn).enumerate().zip(places.iter()) {
            let state = (place.next(offset)).get::<AggregateWindowFunnelState<T::Scalar>>();
            let timestamp = T::to_owned_scalar(timestamp);
            for (i, filter) in dcolumns.iter().enumerate() {
                if filter.get_bit(row) {
                    state.add(timestamp, (i + 1) as u8);
                }
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let tcolumn = T::try_downcast_column(&columns[0]).unwrap();
        let timestamp = unsafe { T::index_column_unchecked(&tcolumn, row) };
        let timestamp = T::to_owned_scalar(timestamp);

        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();
        for i in 0..self.event_size {
            let dcolumn = BooleanType::try_downcast_column(&columns[i + 1]).unwrap();
            if dcolumn.get_bit(row) {
                state.add(timestamp, (i + 1) as u8);
            }
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();
        let mut rhs: AggregateWindowFunnelState<T::Scalar> = deserialize_state(reader)?;
        state.merge(&mut rhs);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();
        let other = rhs.get::<AggregateWindowFunnelState<T::Scalar>>();
        state.merge(other);
        Ok(())
    }

    #[allow(unused_mut)]
    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = UInt8Type::try_downcast_builder(builder).unwrap();
        let result = self.get_event_level(place);
        builder.push(result);
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();
        std::ptr::drop_in_place(state);
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: AggregateFunctionRef,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<AggregateFunctionRef>> {
        Ok(Some(AggregateNullVariadicAdaptor::<false>::create(
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
    T: ArgType + Send + Sync,
    T::Scalar: Number
        + Ord
        + Sub<Output = T::Scalar>
        + AsPrimitive<u64>
        + Clone
        + Serialize
        + DeserializeOwned
        + 'static,
{
    pub fn try_create(
        display_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        let event_size = arguments.len() - 1;
        let window = check_number::<_, u64>(
            None,
            &FunctionContext::default(),
            &Expr::<usize>::Constant {
                span: None,
                scalar: params[0].clone(),
                data_type: params[0].as_ref().infer_data_type(),
            },
            &BUILTIN_FUNCTIONS,
        )?;

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
        let state = place.get::<AggregateWindowFunnelState<T::Scalar>>();
        if state.events_list.is_empty() {
            return 0;
        }
        if self.event_size == 1 {
            return 1;
        }

        state.sort();

        let mut events_timestamp: Vec<Option<T::Scalar>> = Vec::with_capacity(self.event_size);
        for _i in 0..self.event_size {
            events_timestamp.push(None);
        }
        for (timestamp, event) in state.events_list.iter() {
            let event_idx = (event - 1) as usize;

            if event_idx == 0 {
                events_timestamp[event_idx] = Some(timestamp.to_owned());
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

pub fn try_create_aggregate_window_funnel_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_params(display_name, params.len())?;
    assert_variadic_arguments(display_name, arguments.len(), (1, 32))?;

    for (idx, arg) in arguments[1..].iter().enumerate() {
        if !arg.is_boolean() {
            return Err(ErrorCode::BadDataValueType(format!(
                "Illegal type of the argument {:?} in AggregateWindowFunnelFunction, must be boolean, got: {:?}",
                idx + 1,
                arg
            )));
        }
    }

    with_integer_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => AggregateWindowFunnelFunction::<
            NumberType<NUM_TYPE>,
        >::try_create(
            display_name, params, arguments
        ),
        DataType::Date =>
            AggregateWindowFunnelFunction::<DateType>::try_create(display_name, params, arguments),
        DataType::Timestamp => AggregateWindowFunnelFunction::<TimestampType>::try_create(
            display_name,
            params,
            arguments
        ),
        _ => Err(ErrorCode::BadDataValueType(format!(
            "AggregateWindowFunnelFunction does not support type '{:?}'",
            arguments[0]
        ))),
    })
}

pub fn aggregate_window_funnel_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_window_funnel_function))
}
