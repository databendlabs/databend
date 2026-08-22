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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::utils::column_merge_validity;

use super::*;

pub type AggregateMultiArgSkipNullImplementation<I> =
    AggregateMultiArgNullableImplementation<I, false, false>;
pub type AggregateMultiArgOrNullImplementation<I> =
    AggregateMultiArgNullableImplementation<I, true, true>;

pub struct AggregateMultiArgNullableImplementation<
    I,
    const RESULT_NULL: bool,
    const FULL_NULL_FILTER: bool,
> {
    inner: I,
}

impl<I, const RESULT_NULL: bool, const FULL_NULL_FILTER: bool>
    AggregateMultiArgNullableImplementation<I, RESULT_NULL, FULL_NULL_FILTER>
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }

    fn inner_state<'a>(state: AggrState<'a>) -> AggrState<'a> {
        if RESULT_NULL {
            state.remove_last_loc()
        } else {
            state
        }
    }

    fn mark_seen(state: AggrState<'_>) {
        *Self::result_flag(state) = 1;
    }

    fn mark_seen_if_has_rows(state: AggrState<'_>, rows: usize, validity: Option<&Bitmap>) -> bool {
        let has_seen_rows =
            rows > 0 && validity.is_none_or(|validity| validity.null_count() != rows);
        if has_seen_rows {
            Self::mark_seen(state);
        }
        has_seen_rows
    }

    fn seen(state: AggrState<'_>) -> bool {
        *Self::result_flag(state) != 0
    }

    fn merge_result_or_null(
        state: AggrState<'_>,
        builder: &mut ColumnBuilder,
        merge_inner: impl FnOnce(AggrState<'_>, &mut ColumnBuilder) -> Result<()>,
    ) -> Result<()> {
        if !Self::seen(state) {
            builder.push(ScalarRef::Null);
            return Ok(());
        }

        if let ColumnBuilder::Nullable(inner) = builder {
            merge_inner(Self::inner_state(state), &mut inner.builder)?;
            inner.validity.push(true);
            Ok(())
        } else {
            merge_inner(Self::inner_state(state), builder)
        }
    }

    fn result_flag<'a>(state: AggrState<'a>) -> &'a mut u8 {
        state_at(state, state.loc.len() - 1)
    }

    fn strip_nullable_columns(
        columns: ProjectedBlock<'_>,
        validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = validity;
        for entry in columns.iter() {
            validity = if FULL_NULL_FILTER {
                column_merge_validity(entry, validity)
            } else {
                merge_entry_validity(entry, validity)
            };
            not_null_columns.push(entry.clone().remove_nullable());
        }
        (not_null_columns, Bitmap::map_all_sets_to_none(validity))
    }
}

impl<I, const RESULT_NULL: bool, const FULL_NULL_FILTER: bool> AggrImpl
    for AggregateMultiArgNullableImplementation<I, RESULT_NULL, FULL_NULL_FILTER>
where I: AggrImpl
{
    fn init_state(&self, state: AggrState<'_>) {
        if RESULT_NULL {
            *Self::result_flag(state) = 0;
        }
        self.inner.init_state(Self::inner_state(state));
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let rows = input.columns.num_rows();
        let (columns, validity) =
            Self::strip_nullable_columns(input.columns, input.validity.cloned());
        if RESULT_NULL && !Self::mark_seen_if_has_rows(input.state, rows, validity.as_ref()) {
            return Ok(());
        }
        self.inner.accumulate(AccumulateInput {
            state: Self::inner_state(input.state),
            columns: (&columns).into(),
            validity: validity.as_ref(),
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let (columns, validity) = Self::strip_nullable_columns(input.columns, None);
        let columns: ProjectedBlock<'_> = (&columns).into();
        if let Some(validity) = validity {
            for (row, state) in input.states.iter().enumerate() {
                if validity.get(row).unwrap() {
                    if RESULT_NULL {
                        Self::mark_seen(state);
                    }
                    self.inner.accumulate_row(AccumulateRowInput {
                        state: Self::inner_state(state),
                        columns,
                        row,
                    })?;
                }
            }
            return Ok(());
        }

        if RESULT_NULL {
            self.inner.accumulate_keys(AccumulateKeysInput {
                states: input.states.without_last_loc(),
                columns,
            })?;
            for state in input.states.iter() {
                Self::mark_seen(state);
            }
            Ok(())
        } else {
            self.inner.accumulate_keys(AccumulateKeysInput {
                states: input.states,
                columns,
            })
        }
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let (columns, validity) = Self::strip_nullable_columns(input.columns, None);
        if validity
            .as_ref()
            .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }

        if RESULT_NULL {
            Self::mark_seen(input.state);
        }
        self.inner.accumulate_row(AccumulateRowInput {
            state: Self::inner_state(input.state),
            columns: (&columns).into(),
            row: input.row,
        })
    }

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        if RESULT_NULL {
            if input.rows > 0 {
                Self::mark_seen(input.state);
                self.inner.accumulate_row_count(AccumulateRowCountInput {
                    state: Self::inner_state(input.state),
                    rows: input.rows,
                })?;
            }
            return Ok(());
        }
        if input.rows == 0 {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(
                "aggregate does not support rows-only input",
            ))
        }
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        if RESULT_NULL {
            self.inner
                .accumulate_row_count_keys(AccumulateRowCountKeysInput {
                    states: input.states.without_last_loc(),
                })?;
            for state in input.states.iter() {
                Self::mark_seen(state);
            }
        } else {
            for state in input.states.iter() {
                self.accumulate_row_count(AccumulateRowCountInput { state, rows: 1 })?;
            }
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        if RESULT_NULL {
            let (inner_builders, flag_builder) =
                input.builders.split_at_mut(input.builders.len() - 1);
            for state in input.states.iter() {
                flag_builder[0].push(ScalarRef::Boolean(Self::seen(state)));
            }
            self.inner.serialize(SerializeInput {
                states: input.states.without_last_loc(),
                builders: inner_builders,
            })
        } else {
            self.inner.serialize(input)
        }
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        if RESULT_NULL {
            let field_count = serialized_field_count(input.state);
            let flag_field = field_count - 1;
            let flag_filter =
                combined_serialized_flag_filter(input.state, input.filter, flag_field);
            for (row, state) in input.states.iter().enumerate() {
                if flag_filter
                    .as_ref()
                    .is_none_or(|filter| filter.get(row).unwrap())
                {
                    Self::mark_seen(state);
                }
            }
            let inner_state = project_serialized_fields(input.state, 0, flag_field);
            self.inner.merge_serialized(MergeSerializedInput {
                states: input.states.without_last_loc(),
                state: &inner_state,
                filter: flag_filter.as_ref(),
            })
        } else {
            self.inner.merge_serialized(input)
        }
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        if RESULT_NULL && Self::seen(input.rhs) {
            Self::mark_seen(input.state);
        }
        self.inner.merge_states(MergeStatesInput {
            state: Self::inner_state(input.state),
            rhs: Self::inner_state(input.rhs),
        })
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        if RESULT_NULL {
            Self::merge_result_or_null(input.state, input.builder, |state, builder| {
                self.inner.merge_result(MergeResultInput { state, builder })
            })
        } else {
            self.inner.merge_result(input)
        }
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        if RESULT_NULL {
            Self::merge_result_or_null(input.state, input.builder, |state, builder| {
                self.inner
                    .merge_result_read_only(MergeResultInput { state, builder })
            })
        } else {
            self.inner.merge_result_read_only(input)
        }
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.inner.drop_state(Self::inner_state(state)) };
    }
}

fn merge_entry_validity(entry: &BlockEntry, validity: Option<Bitmap>) -> Option<Bitmap> {
    let entry_validity = match entry {
        BlockEntry::Const(scalar, _, rows) if scalar.is_null() => Some(Bitmap::new_zeroed(*rows)),
        BlockEntry::Column(Column::Null { len }) => Some(Bitmap::new_zeroed(*len)),
        BlockEntry::Column(Column::Nullable(column)) => {
            let validity = column.validity();
            (validity.null_count() != 0).then(|| validity.clone())
        }
        _ => None,
    };

    match (validity, entry_validity) {
        (Some(left), Some(right)) => Some(&left & &right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
