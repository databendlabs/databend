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
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::utils::column_merge_validity;

use super::*;

pub(crate) trait UnaryState<I, R>: Send + 'static
where
    I: AccessType,
    R: ValueType,
{
    type FunctionInfo: Send + Sync + 'static;

    fn init(function_info: &Self::FunctionInfo) -> Self;

    fn add(&mut self, value: I::ScalarRef<'_>, function_info: &Self::FunctionInfo) -> Result<()>;

    fn add_batch(
        &mut self,
        values: ColumnView<I>,
        validity: Option<&Bitmap>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        self.add(value, function_info)?;
                    }
                }
            }
            None => {
                for value in values.iter() {
                    self.add(value, function_info)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()>;

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge(rhs)
    }

    fn merge_result(
        &mut self,
        builder: R::ColumnBuilderMut<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;

    /// # Safety
    /// The caller must ensure the state is initialized and owned by this
    /// aggregate instance.
    unsafe fn drop_state(state: &mut Self, _function_info: &Self::FunctionInfo) {
        unsafe { std::ptr::drop_in_place(state) };
    }
}

pub(crate) struct UnaryStateEval<S, I, R, const MAYBE_NULL: bool>
where
    S: UnaryState<I, R>,
    I: AccessType,
    R: ValueType,
{
    function_info: Arc<S::FunctionInfo>,
    _p: PhantomData<fn(S, I, R)>,
}

pub(crate) struct UnaryAccumulateInput<'a> {
    pub(crate) state: AggrState<'a>,
    pub(crate) column: &'a BlockEntry,
    pub(crate) validity: Option<&'a Bitmap>,
}

pub(crate) struct UnaryAccumulateKeysInput<'a> {
    pub(crate) states: AggregateStateSet<'a>,
    pub(crate) column: &'a BlockEntry,
}

pub(crate) struct UnaryAccumulateRowInput<'a> {
    pub(crate) state: AggrState<'a>,
    pub(crate) column: &'a BlockEntry,
    pub(crate) row: usize,
}

pub(crate) trait UnaryEval<I, R>: Send + Sync + 'static
where
    I: AccessType,
    R: ValueType,
{
    fn init_state(&self, state: AggrState<'_>);

    fn accumulate(&self, input: UnaryAccumulateInput<'_>) -> Result<()>;

    fn accumulate_keys(&self, input: UnaryAccumulateKeysInput<'_>) -> Result<()>;

    fn accumulate_row(&self, input: UnaryAccumulateRowInput<'_>) -> Result<()>;

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()>;

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()>;

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()>;

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()>;

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>);
}

pub(crate) struct UnaryEvalAdapter<I, R, U>
where
    I: AccessType,
    R: ValueType,
    U: UnaryEval<I, R>,
{
    nested: U,
    _p: PhantomData<fn(I, R)>,
}

impl<I, R, U> UnaryEvalAdapter<I, R, U>
where
    I: AccessType,
    R: ValueType,
    U: UnaryEval<I, R>,
{
    pub fn new(nested: U) -> Self {
        Self {
            nested,
            _p: PhantomData,
        }
    }
}

impl<S, I, R, const MAYBE_NULL: bool> UnaryStateEval<S, I, R, MAYBE_NULL>
where
    S: UnaryState<I, R>,
    I: AccessType,
    R: ValueType,
{
    pub(crate) fn new(function_info: Arc<S::FunctionInfo>) -> Self {
        Self {
            function_info,
            _p: PhantomData,
        }
    }
}

pub(crate) fn create_unary_distinct_or_null_aggregate_function<S, I, R, C>(
    combinator: C,
    signature: AggregateSignature,
    metadata: AggregateMetadata,
    state: AggregateStateDescription,
    function_info: S::FunctionInfo,
    distinct_args_type: Vec<DataType>,
) -> Result<AggregateCallRef>
where
    S: UnaryState<I, R>,
    I: AccessType,
    R: ValueType,
    C: Combinator,
{
    debug_assert_eq!(distinct_args_type.len(), 1);
    let nested = UnaryEvalAdapter::new(UnaryStateEval::<S, I, R, false>::new(Arc::new(
        function_info,
    )));
    let (state, eval) =
        create_unary_distinct::<false>(nested, &state, distinct_args_type[0].clone());
    let state = state.with_null_flag();
    let eval = MultiArgSkipNullEval::new(MultiArgOrNullEval::new(eval));
    combinator.create::<false>(signature, metadata, state, eval)
}

impl<I, R, U> AggregateEval for UnaryEvalAdapter<I, R, U>
where
    I: AccessType,
    R: ValueType,
    U: UnaryEval<I, R>,
{
    fn init_state(&self, state: AggrState<'_>) {
        self.nested.init_state(state)
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        self.nested.accumulate(UnaryAccumulateInput {
            state: input.state,
            column: &input.columns[0],
            validity: input.validity,
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        self.nested.accumulate_keys(UnaryAccumulateKeysInput {
            states: input.states,
            column: &input.columns[0],
        })
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        self.nested.accumulate_row(UnaryAccumulateRowInput {
            state: input.state,
            column: &input.columns[0],
            row: input.row,
        })
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        self.nested.serialize(input)
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        self.nested.merge_serialized(input)
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        self.nested.merge_states(input)
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.nested.merge_result(input)
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.nested.merge_result_read_only(input)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.nested.drop_state(state) };
    }
}

impl<S, I, R, const MAYBE_NULL: bool> UnaryEval<I, R> for UnaryStateEval<S, I, R, MAYBE_NULL>
where
    S: UnaryState<I, R>,
    I: AccessType,
    R: ValueType,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(|| S::init(&self.function_info));
    }

    fn accumulate(&self, input: UnaryAccumulateInput<'_>) -> Result<()> {
        let entry = input.column;
        let validity = if MAYBE_NULL {
            Bitmap::map_all_sets_to_none(column_merge_validity(entry, input.validity.cloned()))
        } else {
            input.validity.cloned()
        };
        let values = if MAYBE_NULL {
            entry.clone().remove_nullable()
        } else {
            entry.clone()
        }
        .downcast::<I>()
        .unwrap();
        let state = input.state.get::<S>();
        state.add_batch(values, validity.as_ref(), &self.function_info)
    }

    fn accumulate_keys(&self, input: UnaryAccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            self.accumulate_row(UnaryAccumulateRowInput {
                state,
                column: input.column,
                row,
            })?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: UnaryAccumulateRowInput<'_>) -> Result<()> {
        let entry = input.column;
        if MAYBE_NULL {
            let validity = column_merge_validity(entry, None);
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get(input.row).unwrap())
            {
                return Ok(());
            }
        }

        let values = if MAYBE_NULL {
            entry.clone().remove_nullable()
        } else {
            entry.clone()
        }
        .downcast::<I>()
        .unwrap();
        let value = values.index(input.row).unwrap();
        let state = input.state.get::<S>();
        state.add(value, &self.function_info)
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            let state = state.get::<S>();
            state.serialize(&mut input.builders[0], &self.function_info)?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let state = state.get::<S>();
            state.merge_serialized(
                super::serialized_scalar_at(input.state, row, 0),
                &self.function_info,
            )?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<S>();
        state.merge_owned(input.rhs.get::<S>())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<S>();
        let builder = R::downcast_builder(input.builder);
        state.merge_result(builder, &self.function_info)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        let state = state.get::<S>();
        unsafe { S::drop_state(state, &self.function_info) };
    }
}
