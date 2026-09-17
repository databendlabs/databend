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

use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::ValueType;

use super::*;

pub(in crate::aggregates::function_impl) struct RowUniqSet {
    keys: HashSet<Vec<u8>>,
}

impl RowUniqSet {
    pub(in crate::aggregates::function_impl) fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }
    pub(in crate::aggregates::function_impl) fn len(&self) -> usize {
        self.keys.len()
    }
    pub(in crate::aggregates::function_impl) fn serde_item() -> StateSerdeItem {
        StateSerdeItem::DataType(ArrayType::<BinaryType>::data_type())
    }

    pub(in crate::aggregates::function_impl) fn merge(&mut self, rhs: &Self) {
        self.keys.extend(rhs.keys.iter().cloned());
    }
    pub(in crate::aggregates::function_impl) fn serialize(
        &self,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let mut builder = ArrayType::<BinaryType>::downcast_builder(builder);
        for key in &self.keys {
            builder.put_item(key);
        }
        builder.commit_row();
        Ok(())
    }
    pub(in crate::aggregates::function_impl) fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
    ) -> Result<()> {
        let ScalarRef::Array(values) = value else {
            unreachable!()
        };
        let values = BinaryType::try_downcast_column(&values).unwrap();
        self.keys
            .extend(BinaryType::iter_column(&values).map(<[u8]>::to_vec));
        Ok(())
    }

    pub(in crate::aggregates::function_impl) fn build_columns(
        &self,
        types: &[DataType],
    ) -> Result<Vec<BlockEntry>> {
        let mut builders = types
            .iter()
            .map(|ty| ColumnBuilder::with_capacity(ty, self.keys.len()))
            .collect::<Vec<_>>();
        for key in &self.keys {
            let values: Vec<Scalar> = borsh::from_slice(key)?;
            for (builder, value) in builders.iter_mut().zip(values) {
                builder.push(value.as_ref());
            }
        }
        Ok(builders
            .into_iter()
            .map(|builder| builder.build().into())
            .collect())
    }

    pub(in crate::aggregates::function_impl) fn add_row(
        &mut self,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        let values = columns
            .iter()
            .map(|column| column.index(row).unwrap().to_owned())
            .collect::<Vec<Scalar>>();
        self.keys.insert(borsh::to_vec(&values)?);
        Ok(())
    }
}

struct MultiArgDistinctState {
    keys: RowUniqSet,
    replayed: bool,
    populated: bool,
}

struct MultiArgDistinctEval {
    nested: Box<dyn AggregateEval>,
    args_type: Vec<DataType>,
}

pub(super) fn create_multi_arg_distinct(
    signature: AggregateSignature,
    metadata: AggregateMetadata,
    nested: impl AggregateEval,
    state: AggregateStateDescription,
    args_type: Vec<DataType>,
) -> Result<AggregateCallRef> {
    let mut fields = vec![AggrStateType::Custom(Layout::new::<MultiArgDistinctState>())];
    fields.extend_from_slice(state.fields());
    let state = AggregateStateDescription::new(fields, vec![RowUniqSet::serde_item()])
        .with_manual_drop(true);
    PlainCombinator.create::<false>(
        signature,
        metadata,
        state,
        MultiArgSkipNullEval::new(MultiArgDistinctEval {
            nested: Box::new(nested),
            args_type,
        }),
    )
}

impl MultiArgDistinctEval {
    fn state(state: AggrState<'_>) -> &mut MultiArgDistinctState {
        state_at(state, 0)
    }

    fn inner_state<'a>(state: AggrState<'a>) -> AggrState<'a> {
        state.remove_first_loc()
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
        let entries = distinct_state.keys.build_columns(&self.args_type)?;
        self.nested.accumulate(AccumulateInput {
            state: inner,
            columns: entries.as_slice().into(),
            validity: None,
        })?;
        distinct_state.replayed = true;
        Ok(())
    }
}

impl AggregateEval for MultiArgDistinctEval {
    fn init_state(&self, state: AggrState<'_>) {
        write_state_at(state, 0, MultiArgDistinctState {
            keys: RowUniqSet::new(),
            replayed: false,
            populated: false,
        });
        self.nested.init_state(Self::inner_state(state));
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = Self::state(input.state);
        state.replayed = false;
        try_for_each_selected(0..input.columns.num_rows(), input.validity, |row| {
            state.keys.add_row(input.columns, row)
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        try_for_each_selected(
            input.states.iter().enumerate(),
            input.validity,
            |(row, state)| {
                let state = Self::state(state);
                state.replayed = false;
                state.keys.add_row(input.columns, row)
            },
        )
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = Self::state(input.state);
        state.replayed = false;
        state.keys.add_row(input.columns, input.row)
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            Self::state(state).keys.serialize(&mut input.builders[0])?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        try_for_each_selected(
            input.states.iter().enumerate(),
            input.filter,
            |(row, state)| {
                let state = Self::state(state);
                state.replayed = false;
                state
                    .keys
                    .merge_serialized(serialized_scalar_at(input.state, row, 0))
                    .map(|_| ())
            },
        )
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = Self::state(input.state);
        state.replayed = false;
        state.keys.merge(&Self::state(input.rhs).keys);
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

pub(in crate::aggregates::function_impl) struct RowUniqEval;

impl AggregateEval for RowUniqEval {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(RowUniqSet::new);
    }
    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let set = input.state.get::<RowUniqSet>();
        try_for_each_selected(0..input.columns.num_rows(), input.validity, |row| {
            set.add_row(input.columns, row)
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        try_for_each_selected(
            input.states.iter().enumerate(),
            input.validity,
            |(row, state)| state.get::<RowUniqSet>().add_row(input.columns, row),
        )
    }
    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        input
            .state
            .get::<RowUniqSet>()
            .add_row(input.columns, input.row)
    }
    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<RowUniqSet>()
                .serialize(&mut input.builders[0])?;
        }
        Ok(())
    }
    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        try_for_each_selected(
            input.states.iter().enumerate(),
            input.filter,
            |(row, state)| {
                state
                    .get::<RowUniqSet>()
                    .merge_serialized(serialized_scalar_at(input.state, row, 0))
            },
        )
    }
    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<RowUniqSet>()
            .merge(input.rhs.get::<RowUniqSet>());
        Ok(())
    }
    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result_read_only(input)
    }
    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        input.builder.push(ScalarRef::Number(NumberScalar::UInt64(
            input.state.get::<RowUniqSet>().len() as u64,
        )));
        Ok(())
    }
    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<RowUniqSet>()) };
    }
}
