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
use std::collections::HashSet;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::ValueType;
use databend_common_expression::utils::column_merge_validity;

use super::*;

#[derive(Default)]
pub struct AggregateDistinctState {
    keys: HashSet<Vec<u8>>,
    replayed: bool,
}

pub struct AggregateDistinctImplementation<const SKIP_NULLS: bool = false> {
    inner: Box<dyn AggrImpl>,
    args_type: Vec<DataType>,
}

impl<const SKIP_NULLS: bool> AggregateDistinctImplementation<SKIP_NULLS> {
    pub fn new(inner: impl AggrImpl, args_type: Vec<DataType>) -> Self {
        Self {
            inner: Box::new(inner),
            args_type,
        }
    }

    fn state(state: AggrState<'_>) -> &mut AggregateDistinctState {
        state_at(state, 0)
    }

    fn inner_state<'a>(state: AggrState<'a>) -> AggrState<'a> {
        state.remove_first_loc()
    }

    fn inner_states<'a>(states: AggregateStateSet<'a>) -> AggregateStateSet<'a> {
        states.without_first_loc()
    }

    fn strip_nullable_columns(
        columns: ProjectedBlock<'_>,
        validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = validity;
        for entry in columns.iter() {
            validity = column_merge_validity(entry, validity);
            not_null_columns.push(entry.clone().remove_nullable());
        }
        (not_null_columns, Bitmap::map_all_sets_to_none(validity))
    }

    fn prepare_columns(
        columns: ProjectedBlock<'_>,
        validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        if SKIP_NULLS {
            Self::strip_nullable_columns(columns, validity)
        } else {
            (columns.iter().cloned().collect(), validity)
        }
    }

    fn row(columns: ProjectedBlock<'_>, row: usize) -> Vec<Scalar> {
        columns
            .iter()
            .map(|entry| entry.index(row).unwrap().to_owned())
            .collect()
    }

    fn add_row(
        state: &mut AggregateDistinctState,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        let row = Self::row(columns, row);
        let mut key = Vec::with_capacity(row.len() * std::mem::size_of::<Scalar>());
        row.serialize(&mut key)?;
        Self::add_key(state, key);
        Ok(())
    }

    fn add_key(state: &mut AggregateDistinctState, key: Vec<u8>) {
        if state.keys.insert(key) {
            state.replayed = false;
        }
    }

    fn replay_keys(&self, state: AggrState<'_>) -> Result<()> {
        let distinct_state = Self::state(state);
        if distinct_state.replayed {
            return Ok(());
        }

        for key in &distinct_state.keys {
            let mut key_slice = key.as_slice();
            let row = Vec::<Scalar>::deserialize(&mut key_slice)?;
            let entries = row
                .iter()
                .zip(&self.args_type)
                .map(|(scalar, data_type)| {
                    BlockEntry::new_const_column(data_type.clone(), scalar.clone(), 1)
                })
                .collect::<Vec<_>>();
            self.inner.accumulate_row(AccumulateRowInput {
                state: Self::inner_state(state),
                columns: (&entries).into(),
                row: 0,
            })?;
        }
        distinct_state.replayed = true;
        Ok(())
    }
}

impl<const SKIP_NULLS: bool> AggrImpl for AggregateDistinctImplementation<SKIP_NULLS> {
    fn init_state(&self, state: AggrState<'_>) {
        write_state_at(state, 0, AggregateDistinctState::default());
        self.inner.init_state(Self::inner_state(state));
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let (columns, validity) = Self::prepare_columns(input.columns, input.validity.cloned());
        let columns: ProjectedBlock<'_> = (&columns).into();
        let state = Self::state(input.state);
        for row in 0..columns.num_rows() {
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            Self::add_row(state, columns, row)?;
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let (columns, validity) = Self::prepare_columns(input.columns, None);
        let columns: ProjectedBlock<'_> = (&columns).into();
        for (row, state) in input.states.iter().enumerate() {
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            Self::add_row(Self::state(state), columns, row)?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let (columns, validity) = Self::prepare_columns(input.columns, None);
        if validity
            .as_ref()
            .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }
        Self::add_row(Self::state(input.state), (&columns).into(), input.row)
    }

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        self.inner.accumulate_row_count(AccumulateRowCountInput {
            state: Self::inner_state(input.state),
            rows: input.rows,
        })
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        self.inner
            .accumulate_row_count_keys(AccumulateRowCountKeysInput {
                states: Self::inner_states(input.states),
            })
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let (key_builders, inner_builders) = input.builders.split_at_mut(1);
        let mut key_builder = ArrayType::<BinaryType>::downcast_builder(&mut key_builders[0]);
        for state in input.states.iter() {
            for key in &Self::state(state).keys {
                key_builder.put_item(key);
            }
            key_builder.commit_row();
        }
        self.inner.serialize(SerializeInput {
            states: Self::inner_states(input.states),
            builders: inner_builders,
        })
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let state = Self::state(state);
            let ScalarRef::Array(keys) = serialized_scalar_at(input.state, row, 0) else {
                unreachable!()
            };
            let keys = BinaryType::try_downcast_column(&keys).unwrap();
            for key in BinaryType::iter_column(&keys) {
                Self::add_key(state, key.to_vec());
            }
        }

        let field_count = serialized_field_count(input.state);
        let inner_state = project_serialized_fields(input.state, 1, field_count);
        self.inner.merge_serialized(MergeSerializedInput {
            states: Self::inner_states(input.states),
            state: &inner_state,
            filter: input.filter,
        })
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = Self::state(input.state);
        let rhs = Self::state(input.rhs);
        for key in rhs.keys.drain() {
            Self::add_key(state, key);
        }
        rhs.replayed = true;
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.replay_keys(input.state)?;
        self.inner.merge_result(MergeResultInput {
            state: Self::inner_state(input.state),
            builder: input.builder,
        })
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.replay_keys(input.state)?;
        self.inner.merge_result_read_only(MergeResultInput {
            state: Self::inner_state(input.state),
            builder: input.builder,
        })
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(Self::state(state)) };
        unsafe { self.inner.drop_state(Self::inner_state(state)) };
    }
}

pub(crate) fn distinct_state_description(
    state: &AggregateStateDescription,
) -> AggregateStateDescription {
    let mut fields = Vec::with_capacity(state.fields().len() + 1);
    fields.push(AggrStateType::Custom(
        Layout::new::<AggregateDistinctState>(),
    ));
    fields.extend_from_slice(state.fields());

    let mut serde_items = Vec::with_capacity(state.serde_items().len() + 1);
    serde_items.push(StateSerdeItem::DataType(DataType::Array(Box::new(
        DataType::Binary,
    ))));
    serde_items.extend_from_slice(state.serde_items());

    AggregateStateDescription::new(fields, serde_items).with_manual_drop(true)
}
