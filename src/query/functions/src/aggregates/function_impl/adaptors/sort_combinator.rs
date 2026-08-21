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

use std::collections::HashSet;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use itertools::Itertools;

use super::*;

#[derive(Default)]
pub struct AggregateSortState {
    columns: Vec<ColumnBuilder>,
}

pub struct AggregateSortImplementation<I> {
    inner: I,
    input_types: Vec<DataType>,
    order_by: Vec<AggregateRuntimeOrderByItem>,
}

impl<I> AggregateSortImplementation<I> {
    pub fn new(
        inner: I,
        input_types: Vec<DataType>,
        order_by: Vec<AggregateRuntimeOrderByItem>,
    ) -> Self {
        Self {
            inner,
            input_types,
            order_by,
        }
    }

    fn state(state: AggrState<'_>) -> &mut AggregateSortState {
        state_at(state, 0)
    }

    fn init_columns(&self, state: &mut AggregateSortState, rows: usize) {
        if !state.columns.is_empty() {
            return;
        }
        state.columns = self
            .input_types
            .iter()
            .map(|data_type| ColumnBuilder::with_capacity(data_type, rows))
            .collect();
    }

    fn append_rows(
        &self,
        state: &mut AggregateSortState,
        columns: ProjectedBlock<'_>,
        validity: Option<&Bitmap>,
    ) {
        let rows = columns.num_rows();
        self.init_columns(state, rows);
        match validity {
            Some(validity) if validity.null_count() > 0 => {
                for row in 0..rows {
                    if validity.get(row).unwrap() {
                        Self::append_row_to_builders(&mut state.columns, columns, row);
                    }
                }
            }
            _ => {
                for (entry, builder) in columns.iter().zip(&mut state.columns) {
                    match entry {
                        BlockEntry::Const(scalar, _, rows) => {
                            builder.push_repeat(&scalar.as_ref(), *rows);
                        }
                        BlockEntry::Column(column) => {
                            builder.append_column(column);
                        }
                    }
                }
            }
        }
    }

    fn append_row_to_builders(
        builders: &mut [ColumnBuilder],
        columns: ProjectedBlock<'_>,
        row: usize,
    ) {
        for (entry, builder) in columns.iter().zip(builders) {
            builder.push(entry.index(row).unwrap());
        }
    }

    fn merge_columns(state: &mut AggregateSortState, columns: Vec<Column>) {
        if state.columns.is_empty() {
            state.columns = columns
                .into_iter()
                .map(ColumnBuilder::from_column)
                .collect();
            return;
        }
        for (builder, column) in state.columns.iter_mut().zip(columns) {
            builder.append_column(&column);
        }
    }

    fn merge_column_builders(state: &mut AggregateSortState, columns: Vec<ColumnBuilder>) {
        if state.columns.is_empty() {
            state.columns = columns;
            return;
        }
        for (builder, column) in state.columns.iter_mut().zip(columns) {
            builder.append_column(&column.build());
        }
    }

    fn prepare_inner_state<'a>(&self, state: AggrState<'a>) -> Result<AggrState<'a>>
    where I: AggrImpl {
        let sort_state = Self::state(state);
        let inner_state = state.remove_first_loc();
        if sort_state.columns.is_empty() {
            return Ok(inner_state);
        }

        let num_rows = sort_state.columns[0].len();
        if num_rows == 0 {
            return Ok(inner_state);
        }

        let block = DataBlock::new(
            sort_state
                .columns
                .iter()
                .map(|builder| builder.clone().build().into())
                .collect(),
            num_rows,
        );

        let mut skip_offsets = HashSet::with_capacity(self.order_by.len());
        let sort_descs = self
            .order_by
            .iter()
            .map(|item| {
                let offset = match item.input {
                    AggregateRuntimeOrderByInput::Argument { offset } => offset,
                    AggregateRuntimeOrderByInput::SortKey { offset } => {
                        skip_offsets.insert(offset);
                        offset
                    }
                };
                SortColumnDescription {
                    offset,
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                }
            })
            .collect::<Vec<_>>();

        let block = DataBlock::sort(&block, &sort_descs, None)?;
        let args = (0..block.num_columns())
            .filter(|index| !skip_offsets.contains(index))
            .collect_vec();
        self.inner.init_state(inner_state);
        self.inner.accumulate(AccumulateInput {
            state: inner_state,
            columns: ProjectedBlock::project(&args, &block),
            validity: None,
        })?;
        Ok(inner_state)
    }
}

pub(crate) fn sort_runtime_inputs(
    args_type: &[DataType],
    order_by: &[AggregateBoundOrderByItem],
) -> (Vec<DataType>, Vec<AggregateRuntimeOrderByItem>) {
    let mut input_types = args_type.to_vec();
    let mut next_sort_key_offset = args_type.len();
    let order_by = order_by
        .iter()
        .map(|item| {
            let input = match item.source {
                AggregateBoundOrderBySource::Argument { index } => {
                    AggregateRuntimeOrderByInput::Argument { offset: index }
                }
                AggregateBoundOrderBySource::Derived => {
                    let offset = next_sort_key_offset;
                    next_sort_key_offset += 1;
                    input_types.push(item.data_type.clone());
                    AggregateRuntimeOrderByInput::SortKey { offset }
                }
            };

            AggregateRuntimeOrderByItem {
                input,
                data_type: item.data_type.clone(),
                asc: item.asc,
                nulls_first: item.nulls_first,
            }
        })
        .collect();

    (input_types, order_by)
}

pub(crate) fn sort_state_description(
    inner: &AggregateStateDescription,
) -> AggregateStateDescription {
    let mut fields = Vec::with_capacity(inner.fields().len() + 1);
    fields.push(AggrStateType::Custom(std::alloc::Layout::new::<
        AggregateSortState,
    >()));
    fields.extend_from_slice(inner.fields());

    let mut serde_items = Vec::with_capacity(inner.serde_items().len() + 1);
    serde_items.push(StateSerdeItem::DataType(DataType::Binary));
    serde_items.extend_from_slice(inner.serde_items());

    AggregateStateDescription::new(fields, serde_items).with_manual_drop(true)
}

impl<I> AggrImpl for AggregateSortImplementation<I>
where I: AggrImpl
{
    fn init_state(&self, state: AggrState<'_>) {
        write_state_at(state, 0, AggregateSortState::default());
        self.inner.init_state(state.remove_first_loc());
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        self.append_rows(Self::state(input.state), input.columns, input.validity);
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            let state = Self::state(state);
            self.init_columns(state, input.states.len());
            Self::append_row_to_builders(&mut state.columns, input.columns, row);
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = Self::state(input.state);
        self.init_columns(state, input.columns[0].len());
        Self::append_row_to_builders(&mut state.columns, input.columns, input.row);
        Ok(())
    }

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        if input.rows == 0 {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(
                "sorted aggregate does not support rows-only input",
            ))
        }
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        if input.states.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(
                "sorted aggregate does not support rows-only input",
            ))
        }
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let (sort_builders, inner_builders) = input.builders.split_at_mut(1);
        for state in input.states.iter() {
            let state = Self::state(state);
            let columns = state
                .columns
                .iter()
                .map(|builder| builder.clone().build())
                .collect::<Vec<_>>();
            let mut data = Vec::new();
            columns.serialize(&mut data)?;
            sort_builders[0].push(ScalarRef::Binary(&data));
        }
        self.inner.serialize(SerializeInput {
            states: input.states.without_first_loc(),
            builders: inner_builders,
        })
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let mut data = serialized_binary_at(input.state, row, 0);
            let columns = Vec::<Column>::deserialize(&mut data)?;
            Self::merge_columns(Self::state(state), columns);
        }

        let field_count = serialized_field_count(input.state);
        let inner_state = project_serialized_fields(input.state, 1, field_count);
        self.inner.merge_serialized(MergeSerializedInput {
            states: input.states.without_first_loc(),
            state: &inner_state,
            filter: input.filter,
        })
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let rhs_columns = std::mem::take(&mut Self::state(input.rhs).columns);
        Self::merge_column_builders(Self::state(input.state), rhs_columns);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let inner_state = self.prepare_inner_state(input.state)?;
        self.inner.merge_result(MergeResultInput {
            state: inner_state,
            builder: input.builder,
        })
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let inner_state = self.prepare_inner_state(input.state)?;
        self.inner.merge_result_read_only(MergeResultInput {
            state: inner_state,
            builder: input.builder,
        })
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(Self::state(state)) };
        unsafe { self.inner.drop_state(state.remove_first_loc()) };
    }
}
