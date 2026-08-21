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
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ValueType;
use databend_common_expression::utils::column_merge_validity;

use super::unary::UnaryAccumulateInput;
use super::unary::UnaryAccumulateKeysInput;
use super::unary::UnaryAccumulateRowInput;
use super::unary::UnaryAggrImpl;
use super::*;

pub(crate) type UnarySkipNull<U> = UnaryNullable<U, false>;
pub(crate) type UnaryOrNull<U> = UnaryNullable<U, true>;

pub(crate) struct UnaryNullable<U, const RESULT_NULL: bool> {
    inner: U,
}

impl<U, const RESULT_NULL: bool> UnaryNullable<U, RESULT_NULL> {
    pub fn new(inner: U) -> Self {
        Self { inner }
    }

    fn flag(state: AggrState<'_>) -> &mut u8 {
        debug_assert!(RESULT_NULL);
        state_at(state, state.loc.len() - 1)
    }

    fn mark_seen(rows: usize, validity: Option<&Bitmap>) -> bool {
        debug_assert!(RESULT_NULL);
        rows > 0 && validity.is_none_or(|validity| validity.null_count() != rows)
    }

    fn inner_state<'a>(state: AggrState<'a>) -> AggrState<'a> {
        if RESULT_NULL {
            state.remove_last_loc()
        } else {
            state
        }
    }

    fn strip_nullable_column(
        column: &BlockEntry,
        validity: Option<Bitmap>,
    ) -> (BlockEntry, Option<Bitmap>) {
        let validity = if RESULT_NULL {
            column_merge_validity(column, validity)
        } else {
            merge_nullable_validity(column, validity)
        };
        (
            column.clone().remove_nullable(),
            Bitmap::map_all_sets_to_none(validity),
        )
    }
}

fn merge_nullable_validity(column: &BlockEntry, validity: Option<Bitmap>) -> Option<Bitmap> {
    let column_validity = match column {
        BlockEntry::Const(scalar, _, rows) if scalar.is_null() => Some(Bitmap::new_zeroed(*rows)),
        BlockEntry::Column(Column::Null { len }) => Some(Bitmap::new_zeroed(*len)),
        BlockEntry::Column(Column::Nullable(column)) => {
            let validity = column.validity();
            (validity.null_count() != 0).then(|| validity.clone())
        }
        _ => None,
    };

    match (validity, column_validity) {
        (Some(left), Some(right)) => Some(&left & &right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

impl<I, R, U, const RESULT_NULL: bool> UnaryAggrImpl<I, R> for UnaryNullable<U, RESULT_NULL>
where
    I: AccessType,
    R: ValueType,
    U: UnaryAggrImpl<I, R>,
{
    fn init_state(&self, state: AggrState<'_>) {
        if RESULT_NULL {
            *Self::flag(state) = 0;
        }
        self.inner.init_state(Self::inner_state(state));
    }

    fn accumulate(&self, input: UnaryAccumulateInput<'_>) -> Result<()> {
        let rows = input.column.len();
        let (column, validity) = Self::strip_nullable_column(input.column, input.validity.cloned());
        if RESULT_NULL && !Self::mark_seen(rows, validity.as_ref()) {
            return Ok(());
        }
        if RESULT_NULL {
            *Self::flag(input.state) = 1;
        }
        self.inner.accumulate(UnaryAccumulateInput {
            state: Self::inner_state(input.state),
            column: &column,
            validity: validity.as_ref(),
        })
    }

    fn accumulate_keys(&self, input: UnaryAccumulateKeysInput<'_>) -> Result<()> {
        let (column, validity) = Self::strip_nullable_column(input.column, None);
        if let Some(validity) = validity {
            for (row, state) in input.states.iter().enumerate() {
                if validity.get(row).unwrap() {
                    if RESULT_NULL {
                        *Self::flag(state) = 1;
                    }
                    self.inner.accumulate_row(UnaryAccumulateRowInput {
                        state: Self::inner_state(state),
                        column: &column,
                        row,
                    })?;
                }
            }
            return Ok(());
        }

        if RESULT_NULL {
            self.inner.accumulate_keys(UnaryAccumulateKeysInput {
                states: input.states.without_last_loc(),
                column: &column,
            })?;
            for state in input.states.iter() {
                *Self::flag(state) = 1;
            }
            return Ok(());
        }

        self.inner.accumulate_keys(UnaryAccumulateKeysInput {
            states: input.states,
            column: &column,
        })
    }

    fn accumulate_row(&self, input: UnaryAccumulateRowInput<'_>) -> Result<()> {
        let (column, validity) = Self::strip_nullable_column(input.column, None);
        if validity
            .as_ref()
            .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }

        if RESULT_NULL {
            *Self::flag(input.state) = 1;
        }
        self.inner.accumulate_row(UnaryAccumulateRowInput {
            state: Self::inner_state(input.state),
            column: &column,
            row: input.row,
        })
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        if !RESULT_NULL {
            return self.inner.serialize(input);
        }
        let (inner_builders, flag_builder) = input.builders.split_at_mut(input.builders.len() - 1);
        for state in input.states.iter() {
            flag_builder[0].push(ScalarRef::Boolean(*Self::flag(state) != 0));
        }
        self.inner.serialize(SerializeInput {
            states: input.states.without_last_loc(),
            builders: inner_builders,
        })
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        if !RESULT_NULL {
            return self.inner.merge_serialized(input);
        }
        let field_count = serialized_field_count(input.state);
        let flag_field = field_count - 1;
        let flag_filter = combined_serialized_flag_filter(input.state, input.filter, flag_field);
        for (row, state) in input.states.iter().enumerate() {
            if flag_filter
                .as_ref()
                .is_none_or(|filter| filter.get(row).unwrap())
            {
                *Self::flag(state) = 1;
            }
        }
        let inner_state = project_serialized_fields(input.state, 0, flag_field);
        self.inner.merge_serialized(MergeSerializedInput {
            states: input.states.without_last_loc(),
            state: &inner_state,
            filter: flag_filter.as_ref(),
        })
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        if RESULT_NULL {
            let seen = *Self::flag(input.rhs) != 0;
            if seen {
                *Self::flag(input.state) = 1;
            }
        }
        self.inner.merge_states(MergeStatesInput {
            state: Self::inner_state(input.state),
            rhs: Self::inner_state(input.rhs),
        })
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        if !RESULT_NULL {
            return self.inner.merge_result(input);
        }
        if *Self::flag(input.state) == 0 {
            input.builder.push(ScalarRef::Null);
            return Ok(());
        }
        if let ColumnBuilder::Nullable(inner) = input.builder {
            self.inner.merge_result(MergeResultInput {
                state: input.state.remove_last_loc(),
                builder: &mut inner.builder,
            })?;
            inner.validity.push(true);
            return Ok(());
        }
        self.inner.merge_result(MergeResultInput {
            state: input.state.remove_last_loc(),
            builder: input.builder,
        })
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        if !RESULT_NULL {
            return self.inner.merge_result_read_only(input);
        }
        if *Self::flag(input.state) == 0 {
            input.builder.push(ScalarRef::Null);
            return Ok(());
        }
        if let ColumnBuilder::Nullable(inner) = input.builder {
            self.inner.merge_result_read_only(MergeResultInput {
                state: input.state.remove_last_loc(),
                builder: &mut inner.builder,
            })?;
            inner.validity.push(true);
            return Ok(());
        }
        self.inner.merge_result_read_only(MergeResultInput {
            state: input.state.remove_last_loc(),
            builder: input.builder,
        })
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.inner.drop_state(Self::inner_state(state)) };
    }
}
