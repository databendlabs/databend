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

use std::marker::PhantomData;

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::utils::column_merge_validity;

use super::*;

pub(crate) trait AggregateUnaryState<T>: Clone + Send + Sync + 'static
where T: AccessType
{
    fn state_description(return_type: DataType) -> AggregateStateDescription;

    fn add(&mut self, value: Option<T::ScalarRef<'_>>);

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()>;

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()>;

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()>;

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()>;

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
}

pub(crate) struct AggregateUnaryStateImplementation<T, State> {
    _p: PhantomData<fn(T, State)>,
}

impl<T, State> Default for AggregateUnaryStateImplementation<T, State> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<T, State> AggrImpl for AggregateUnaryStateImplementation<T, State>
where
    T: AccessType,
    State: Default + AggregateUnaryState<T>,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(State::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        let entry = &input.columns[0];
        if entry.data_type().is_null() {
            return Ok(());
        }

        let validity = column_merge_validity(entry, input.validity.cloned());
        let column = entry.clone().remove_nullable().downcast::<T>().unwrap();
        state.add_batch(column, validity.as_ref())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let entry = &input.columns[0];
        if entry.data_type().is_nullable() {
            let values = entry.downcast::<NullableType<T>>().unwrap();
            for (value, state) in values.iter().zip(input.states.iter()) {
                state.get::<State>().add(value);
            }
            return Ok(());
        }

        let values = entry.downcast::<T>().unwrap();
        for (value, state) in values.iter().zip(input.states.iter()) {
            state.get::<State>().add(Some(value));
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        let entry = &input.columns[0];
        if entry.data_type().is_nullable() {
            let values = entry.downcast::<NullableType<T>>().unwrap();
            state.add(values.index(input.row).unwrap());
            return Ok(());
        }

        let values = entry.downcast::<T>().unwrap();
        state.add(Some(values.index(input.row).unwrap()));
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state.get::<State>().serialize(&mut input.builders[0])?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state
                .get::<State>()
                .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<State>()
            .merge_owned(input.rhs.get::<State>())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        state.merge_result(input.builder)
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let mut state = input.state.get::<State>().clone();
        state.merge_result(input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<State>()) };
    }
}
