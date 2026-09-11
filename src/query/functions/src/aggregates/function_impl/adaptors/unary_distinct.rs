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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::types::*;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::with_number_mapped_type;

use super::super::uniq::DistinctSet;
use super::super::uniq::ScalarUniqSet;
use super::super::uniq::StringDistinctSet;
use super::super::uniq::TypedUniqSet;
use super::*;

pub(crate) struct UnaryDistinctState<S> {
    keys: S,
    replayed: bool,
    populated: bool,
}

pub(crate) struct UnaryDistinctEval<S, const SKIP_NULLS: bool> {
    nested: Box<dyn AggregateEval>,
    arg_type: DataType,
    _set: std::marker::PhantomData<fn() -> S>,
}

pub(crate) fn create_unary_distinct<const SKIP_NULLS: bool>(
    nested: impl AggregateEval,
    state: &AggregateStateDescription,
    arg_type: DataType,
) -> (AggregateStateDescription, Box<dyn AggregateEval>) {
    let nested: Box<dyn AggregateEval> = Box::new(nested);
    fn create<S: DistinctSet, const SKIP_NULLS: bool>(
        nested: Box<dyn AggregateEval>,
        state: &AggregateStateDescription,
        arg_type: DataType,
    ) -> (AggregateStateDescription, Box<dyn AggregateEval>) {
        let mut fields = vec![AggrStateType::Custom(Layout::new::<UnaryDistinctState<S>>())];
        fields.extend_from_slice(state.fields());
        let state =
            AggregateStateDescription::new(fields, vec![S::serde_item()]).with_manual_drop(true);
        (
            state,
            Box::new(UnaryDistinctEval::<S, SKIP_NULLS> {
                nested,
                arg_type,
                _set: std::marker::PhantomData,
            }),
        )
    }
    with_number_mapped_type!(|NUM| match arg_type.remove_nullable() {
        DataType::Number(NumberDataType::NUM) =>
            create::<TypedUniqSet<NumberType<NUM>>, SKIP_NULLS>(nested, state, arg_type),
        DataType::Date => create::<TypedUniqSet<DateType>, SKIP_NULLS>(nested, state, arg_type),
        DataType::Timestamp =>
            create::<TypedUniqSet<TimestampType>, SKIP_NULLS>(nested, state, arg_type),
        DataType::String => create::<StringDistinctSet, SKIP_NULLS>(nested, state, arg_type),
        _ => create::<ScalarUniqSet, SKIP_NULLS>(nested, state, arg_type),
    })
}

impl<S: DistinctSet, const SKIP_NULLS: bool> UnaryDistinctEval<S, SKIP_NULLS> {
    fn state(state: AggrState<'_>) -> &mut UnaryDistinctState<S> {
        state_at(state, 0)
    }

    fn inner_state<'a>(state: AggrState<'a>) -> AggrState<'a> {
        state.remove_first_loc()
    }

    fn prepare_column(
        entry: &BlockEntry,
        validity: Option<Bitmap>,
    ) -> (BlockEntry, Option<Bitmap>) {
        if SKIP_NULLS {
            let validity = column_merge_validity(entry, validity);
            (
                entry.clone().remove_nullable(),
                Bitmap::map_all_sets_to_none(validity),
            )
        } else {
            (entry.clone(), validity)
        }
    }

    fn replay_keys(&self, state: AggrState<'_>) -> Result<()> {
        let distinct_state = Self::state(state);
        if distinct_state.replayed {
            return Ok(());
        }

        let inner = Self::inner_state(state);
        // The inner state is a cache of finalized keys. New input or a merge
        // invalidates it; rebuild instead of adding the entire set a second time.
        if distinct_state.populated {
            unsafe {
                self.nested.drop_state(inner);
            }
            self.nested.init_state(inner);
        }
        distinct_state.populated = true;
        let column = distinct_state.keys.build_column(&self.arg_type)?;
        let entries = [BlockEntry::from(column)];
        self.nested.accumulate(AccumulateInput {
            state: inner,
            columns: (&entries).into(),
            validity: None,
        })?;
        distinct_state.replayed = true;
        Ok(())
    }
}

impl<S: DistinctSet, const SKIP_NULLS: bool> AggregateEval for UnaryDistinctEval<S, SKIP_NULLS> {
    fn init_state(&self, state: AggrState<'_>) {
        write_state_at(state, 0, UnaryDistinctState {
            keys: S::new(),
            replayed: false,
            populated: false,
        });
        self.nested.init_state(Self::inner_state(state));
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let (entry, validity) = Self::prepare_column(&input.columns[0], input.validity.cloned());
        let state = Self::state(input.state);
        if state.keys.add_batch(&entry, validity.as_ref())? {
            state.replayed = false;
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let (entry, validity) = Self::prepare_column(&input.columns[0], None);
        let view = entry.downcast::<S::Type>().unwrap();
        for ((row, value), state) in view.iter().enumerate().zip(input.states.iter()) {
            if validity.as_ref().is_some_and(|v| !v.get(row).unwrap()) {
                continue;
            }
            let state = Self::state(state);
            if state.keys.insert(value)? {
                state.replayed = false;
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let (entry, validity) = Self::prepare_column(&input.columns[0], None);
        if validity
            .as_ref()
            .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }
        let state = Self::state(input.state);
        let view = entry.downcast::<S::Type>().unwrap();
        if state.keys.insert(view.index(input.row).unwrap())? {
            state.replayed = false;
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            Self::state(state).keys.serialize(&mut input.builders[0])?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let state = Self::state(state);
            if state
                .keys
                .merge_serialized(serialized_scalar_at(input.state, row, 0))?
            {
                state.replayed = false;
            }
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = Self::state(input.state);
        if state.keys.merge(&Self::state(input.rhs).keys) {
            state.replayed = false;
        }
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.replay_keys(input.state)?;
        let result = self.nested.merge_result(MergeResultInput {
            state: Self::inner_state(input.state),
            builder: input.builder,
        });
        Self::state(input.state).replayed = false;
        result
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.replay_keys(input.state)?;
        self.nested.merge_result_read_only(MergeResultInput {
            state: Self::inner_state(input.state),
            builder: input.builder,
        })
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(Self::state(state)) };
        unsafe { self.nested.drop_state(Self::inner_state(state)) };
    }
}
