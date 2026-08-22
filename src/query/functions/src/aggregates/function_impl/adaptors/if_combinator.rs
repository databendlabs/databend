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
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::types::BooleanType;
use databend_common_expression::utils::column_merge_validity;

use super::*;

pub(crate) struct AggregateIfImplementation<I> {
    nested: I,
    condition_index: usize,
    always_false: bool,
    strip_nullable_input: bool,
}

impl<I> AggregateIfImplementation<I> {
    pub(crate) fn new(
        nested: I,
        condition_index: usize,
        always_false: bool,
        strip_nullable_input: bool,
    ) -> Self {
        Self {
            nested,
            condition_index,
            always_false,
            strip_nullable_input,
        }
    }

    fn arguments(&self, columns: ProjectedBlock<'_>) -> Vec<BlockEntry> {
        columns.iter().take(self.condition_index).cloned().collect()
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

    fn strip_nullable_condition(
        &self,
        columns: ProjectedBlock<'_>,
        validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        let mut columns = columns.iter().cloned().collect::<Vec<_>>();
        let validity = column_merge_validity(&columns[self.condition_index], validity);
        columns[self.condition_index] = columns[self.condition_index].clone().remove_nullable();
        (columns, Bitmap::map_all_sets_to_none(validity))
    }

    fn prepare_columns(
        &self,
        columns: ProjectedBlock<'_>,
        validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        if self.strip_nullable_input {
            Self::strip_nullable_columns(columns, validity)
        } else {
            self.strip_nullable_condition(columns, validity)
        }
    }

    fn predicate(
        &self,
        columns: ProjectedBlock<'_>,
        validity: Option<&Bitmap>,
    ) -> Option<Option<Bitmap>> {
        let view = columns[self.condition_index]
            .downcast::<BooleanType>()
            .unwrap();
        match view.and_bitmap(validity) {
            ColumnView::Const(true, _) => Some(None),
            ColumnView::Const(false, _) => None,
            ColumnView::Column(predicate) => Some(Some(predicate)),
        }
    }

    fn should_accumulate_row(
        &self,
        columns: ProjectedBlock<'_>,
        validity: Option<&Bitmap>,
        row: usize,
    ) -> bool {
        if validity.is_some_and(|validity| !validity.get(row).unwrap()) {
            return false;
        }
        let predicate = columns[self.condition_index]
            .downcast::<BooleanType>()
            .unwrap();
        predicate.index(row).unwrap()
    }
}

impl<I> AggrImpl for AggregateIfImplementation<I>
where I: AggrImpl
{
    fn init_state(&self, state: AggrState<'_>) {
        self.nested.init_state(state)
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        if self.always_false {
            return Ok(());
        }

        let rows = input.columns.num_rows();
        let (columns, validity) = self.prepare_columns(input.columns, input.validity.cloned());
        let columns: ProjectedBlock<'_> = (&columns).into();
        let Some(predicate) = self.predicate(columns, validity.as_ref()) else {
            return Ok(());
        };
        let args = self.arguments(columns);
        if args.is_empty() {
            let rows = predicate.as_ref().map(Bitmap::true_count).unwrap_or(rows);
            return self.nested.accumulate_row_count(AccumulateRowCountInput {
                state: input.state,
                rows,
            });
        }

        self.nested.accumulate(AccumulateInput {
            state: input.state,
            columns: (&args).into(),
            validity: predicate.as_ref(),
        })
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        if self.always_false {
            return Ok(());
        }

        let (columns, validity) = self.prepare_columns(input.columns, None);
        let columns: ProjectedBlock<'_> = (&columns).into();
        let args = self.arguments(columns);
        let args: ProjectedBlock<'_> = (&args).into();
        for (row, state) in input.states.iter().enumerate() {
            if !self.should_accumulate_row(columns, validity.as_ref(), row) {
                continue;
            }
            if args.is_empty() {
                self.nested
                    .accumulate_row_count(AccumulateRowCountInput { state, rows: 1 })?;
            } else {
                self.nested.accumulate_row(AccumulateRowInput {
                    state,
                    columns: args,
                    row,
                })?;
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        if self.always_false {
            return Ok(());
        }

        let (columns, validity) = self.prepare_columns(input.columns, None);
        let columns: ProjectedBlock<'_> = (&columns).into();
        if !self.should_accumulate_row(columns, validity.as_ref(), input.row) {
            return Ok(());
        }
        let args = self.arguments(columns);
        if args.is_empty() {
            return self.nested.accumulate_row_count(AccumulateRowCountInput {
                state: input.state,
                rows: 1,
            });
        }
        self.nested.accumulate_row(AccumulateRowInput {
            state: input.state,
            columns: (&args).into(),
            row: input.row,
        })
    }

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        self.nested.accumulate_row_count(input)
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        self.nested.accumulate_row_count_keys(input)
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
