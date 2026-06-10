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
use std::marker::PhantomData;
use std::ops::Sub;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::with_integer_mapped_type;
use num_traits::AsPrimitive;

use super::super::extract_number_param;
use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;

struct WindowFunnelBuilder;

impl WindowFunnelBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let window_funnel = AggregateFunctionDefinition::new(
            "window_funnel",
            WindowFunnelBuilder::window_funnel_arguments(),
            WindowFunnelBuilder::WINDOW_FUNNEL_FEATURES,
            WindowFunnelBuilder::try_create,
        );
        window_funnel.register_with_combinators(registry, false);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: WindowFunnelBuilder::register,
    }
}

impl WindowFunnelBuilder {
    fn window_funnel_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::variadic(
            vec![v2::AggregateArgumentPattern::any()],
            v2::AggregateArgumentPattern::exact(DataType::Boolean),
            1,
            Some(32),
        )
    }

    const WINDOW_FUNNEL_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "calculates the maximum event funnel level within a time window",
        definition: "window_funnel(window)(timestamp, event1, ...)",
        example: "select window_funnel(60)(ts, event1, event2) from t",
    };
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct AggregateWindowFunnelState<T> {
    events_list: Vec<(T, u8)>,
    sorted: bool,
}

impl<T> AggregateWindowFunnelState<T>
where T: Copy + Ord
{
    fn new() -> Self {
        Self {
            events_list: Vec::new(),
            sorted: true,
        }
    }

    fn add(&mut self, timestamp: T, event: u8) {
        if self.sorted
            && let Some(last) = self.events_list.last()
        {
            if last.0 == timestamp {
                self.sorted = last.1 <= event;
            } else {
                self.sorted = last.0 <= timestamp;
            }
        }
        self.events_list.push((timestamp, event));
    }

    fn sort(&mut self) {
        if !self.sorted {
            self.events_list.sort_by(compare_event);
            self.sorted = true;
        }
    }

    fn merge_owned(&mut self, rhs: &mut Self) {
        if rhs.events_list.is_empty() {
            return;
        }
        self.sort();
        rhs.sort();

        let mut lhs = std::mem::take(&mut self.events_list);
        let rhs = std::mem::take(&mut rhs.events_list);
        let mut merged = Vec::with_capacity(lhs.len() + rhs.len());
        let mut lhs_iter = lhs.drain(..).peekable();
        let mut rhs_iter = rhs.into_iter().peekable();

        while let (Some(lhs), Some(rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
            if compare_event(lhs, rhs) == Ordering::Less {
                merged.push(lhs_iter.next().unwrap());
            } else {
                merged.push(rhs_iter.next().unwrap());
            }
        }
        merged.extend(lhs_iter);
        merged.extend(rhs_iter);

        self.events_list = merged;
        self.sorted = true;
    }
}

fn compare_event<T: Ord>(lhs: &(T, u8), rhs: &(T, u8)) -> Ordering {
    lhs.0.cmp(&rhs.0).then_with(|| lhs.1.cmp(&rhs.1))
}

impl WindowFunnelBuilder {
    fn try_create(request: v2::AggregateFunctionRequest<'_>) -> Result<v2::AggregateFunctionRef> {
        if request.params.len() != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects one parameter",
                request.name
            )));
        }
        v2::build_default_name_route_with_direct_input(
            request,
            &["window_funnel"],
            Self::WINDOW_FUNNEL_FEATURES,
            false,
            direct_aggregate_function_build_input_fns!(Self::create),
        )
    }

    fn create(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef> {
        for (index, data_type) in build.args_type()[1..].iter().enumerate() {
            if data_type.remove_nullable() != DataType::Boolean {
                return Err(ErrorCode::BadDataValueType(format!(
                    "Illegal type of the argument {} in AggregateWindowFunnelFunction, must be boolean, got: {:?}",
                    index + 1,
                    data_type
                )));
            }
        }
        let window = extract_number_param(build.params()[0].clone())?;
        let timestamp_type = build.args_type()[0].remove_nullable();

        with_integer_mapped_type!(|NUM_TYPE| match &timestamp_type {
            DataType::Number(NumberDataType::NUM_TYPE) => {
                Self::create_instance::<NumberType<NUM_TYPE>>(build, window)
            }
            DataType::Date => Self::create_instance::<DateType>(build, window),
            DataType::Timestamp => Self::create_instance::<TimestampType>(build, window),
            _ => Err(ErrorCode::BadDataValueType(format!(
                "AggregateWindowFunnelFunction does not support type '{:?}'",
                build.args_type()[0]
            ))),
        })
    }

    fn create_instance<T>(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
        window: u64,
    ) -> Result<v2::AggregateFunctionRef>
    where
        T: ArgType,
        T::Scalar: Number
            + Copy
            + Ord
            + Sub<Output = T::Scalar>
            + AsPrimitive<u64>
            + BorshSerialize
            + BorshDeserialize,
    {
        let state = AggregateWindowFunnelImplementation::<T>::state_description();
        let implementation =
            AggregateWindowFunnelImplementation::<T>::new(build.args_type().len() - 1, window);
        build.create(
            UInt8Type::data_type().wrap_nullable(),
            state.with_null_flag(),
            v2::AggregateMultiArgOrNullImplementation::new(implementation),
        )
    }
}

pub struct AggregateWindowFunnelImplementation<T: ArgType> {
    event_size: usize,
    window: u64,
    _t: PhantomData<fn(T)>,
}

impl<T> AggregateWindowFunnelImplementation<T>
where
    T: ArgType,
    T::Scalar: Number
        + Copy
        + Ord
        + Sub<Output = T::Scalar>
        + AsPrimitive<u64>
        + BorshSerialize
        + BorshDeserialize,
{
    fn new(event_size: usize, window: u64) -> Self {
        Self {
            event_size,
            window,
            _t: PhantomData,
        }
    }

    fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<
                AggregateWindowFunnelState<T::Scalar>,
            >())],
            vec![StateSerdeItem::Binary(None)],
        )
        .with_manual_drop(true)
    }

    fn accumulate_row_into_state(
        &self,
        state: &mut AggregateWindowFunnelState<T::Scalar>,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        let timestamp = T::to_owned_scalar(columns[0].downcast::<T>()?.index(row).unwrap());
        for index in 0..self.event_size {
            let event = columns[index + 1].downcast::<BooleanType>()?;
            if event.index(row).unwrap() {
                state.add(timestamp, (index + 1) as u8);
            }
        }
        Ok(())
    }

    fn event_level(&self, state: &mut AggregateWindowFunnelState<T::Scalar>) -> u8 {
        if state.events_list.is_empty() {
            return 0;
        }
        if self.event_size == 1 {
            return 1;
        }
        state.sort();

        let mut events_timestamp = vec![None; self.event_size];
        for (timestamp, event) in &state.events_list {
            let event_index = (*event - 1) as usize;
            if event_index == 0 {
                events_timestamp[event_index] = Some(*timestamp);
            } else if let Some(previous) = events_timestamp[event_index - 1] {
                let window: u64 = timestamp.sub(previous).as_();
                if window <= self.window {
                    events_timestamp[event_index] = events_timestamp[event_index - 1];
                }
            }
        }

        for index in (0..self.event_size).rev() {
            if events_timestamp[index].is_some() {
                return index as u8 + 1;
            }
        }
        0
    }

    fn window_state<'a>(
        &self,
        state: AggrState<'a>,
    ) -> &'a mut AggregateWindowFunnelState<T::Scalar> {
        state.get::<AggregateWindowFunnelState<T::Scalar>>()
    }

    fn accumulate_seen_row(
        &self,
        state: AggrState<'_>,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        self.accumulate_row_into_state(self.window_state(state), columns, row)
    }

    fn merge_serialized_row(
        &self,
        state: AggrState<'_>,
        serialized_state: &BlockEntry,
        row: usize,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = v2::serialized_scalar_at(serialized_state, row, 0) else {
            unreachable!()
        };
        let mut rhs = AggregateWindowFunnelState::<T::Scalar>::deserialize_reader(&mut data)?;
        self.window_state(state).merge_owned(&mut rhs);
        Ok(())
    }
}

impl<T> v2::AggrImpl for AggregateWindowFunnelImplementation<T>
where
    T: ArgType,
    T::Scalar: Number
        + Copy
        + Ord
        + Sub<Output = T::Scalar>
        + AsPrimitive<u64>
        + BorshSerialize
        + BorshDeserialize,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateWindowFunnelState::<T::Scalar>::new);
    }

    fn accumulate(&self, input: v2::AccumulateInput<'_>) -> Result<()> {
        let rows = input.columns.num_rows();
        let state = self.window_state(input.state);
        for row in 0..rows {
            if input
                .validity
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            self.accumulate_row_into_state(state, input.columns, row)?;
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            self.accumulate_seen_row(state, input.columns, row)?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: v2::AccumulateRowInput<'_>) -> Result<()> {
        self.accumulate_seen_row(input.state, input.columns, input.row)?;
        Ok(())
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        let [state_builder] = input.builders else {
            unreachable!()
        };
        let state_builder = state_builder.as_binary_mut().unwrap();
        for state in input.states.iter() {
            let state = self.window_state(state);
            BorshSerialize::serialize(state, &mut state_builder.data)?;
            state_builder.commit_row();
        }
        Ok(())
    }

    fn merge_serialized(&self, input: v2::MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            self.merge_serialized_row(state, input.state, row)?;
        }
        Ok(())
    }

    fn merge_states(&self, input: v2::MergeStatesInput<'_>) -> Result<()> {
        self.window_state(input.state)
            .merge_owned(self.window_state(input.rhs));
        Ok(())
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        let state = self.window_state(input.state);
        let result = self.event_level(state);
        input
            .builder
            .push(ScalarRef::Number(NumberScalar::UInt8(result)));
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(self.window_state(state)) };
    }
}
