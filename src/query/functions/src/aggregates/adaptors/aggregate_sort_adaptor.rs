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
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use itertools::Itertools;

use super::batch_merge1;
use super::batch_serialize1;
use super::AggregateFunctionSortDesc;
use super::SerializeInfo;
use super::StateSerde;

#[derive(Debug, Clone)]
pub struct SortAggState {
    columns: Vec<ColumnBuilder>,
}

impl StateSerde for SortAggState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<BinaryType, Self, _>(places, &loc[..1], builders, |state, builder| {
            let columns = state
                .columns
                .iter()
                .map(|builder| builder.clone().build()) // todo: this clone needs to be eliminated
                .collect::<Vec<_>>();
            columns.serialize(&mut builder.data)?;
            builder.commit_row();
            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, &loc[..1], state, filter, |state, mut data| {
            let columns: Vec<Column> = BorshDeserialize::deserialize(&mut data)?;
            let rhs: Vec<_> = columns
                .into_iter()
                .map(ColumnBuilder::from_column)
                .collect();
            AggregateFunctionSortAdaptor::merge_states_inner(state, &rhs);
            Ok(())
        })
    }
}

pub struct AggregateFunctionSortAdaptor {
    name: String,
    sort_descs: Vec<AggregateFunctionSortDesc>,
    inner: AggregateFunctionRef,
}

impl AggregateFunction for AggregateFunctionSortAdaptor {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.inner.return_type()
    }

    #[inline]
    fn init_state(&self, place: AggrState) {
        Self::set_state(place, SortAggState { columns: vec![] });
        self.inner.init_state(place.remove_first_loc());
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<SortAggState>()));
        self.inner.register_state(registry);
    }

    fn accumulate(
        &self,
        place: AggrState,
        block: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = Self::get_state(place);
        Self::init_columns(block, state, Some(input_rows));

        match validity {
            Some(validity) => {
                for (view, builder) in block
                    .iter()
                    .map(|entry| entry.downcast::<AnyType>().unwrap())
                    .zip(&mut state.columns)
                {
                    view.iter().zip(validity.iter()).for_each(|(v, b)| {
                        if b {
                            builder.push(v);
                        }
                    });
                }
            }
            None => {
                for (entry, builder) in block.iter().zip(&mut state.columns) {
                    match entry {
                        BlockEntry::Const(scalar, _, n) => {
                            builder.push_repeat(&scalar.as_ref(), *n);
                        }
                        BlockEntry::Column(column) => {
                            builder.append_column(column);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, block: ProjectedBlock, row: usize) -> Result<()> {
        let state = Self::get_state(place);
        Self::init_columns(block, state, None);
        for (entry, builder) in block.iter().zip(&mut state.columns) {
            builder.push(entry.index(row).unwrap())
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        SortAggState::serialize_type(None)
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        SortAggState::batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        SortAggState::batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = Self::get_state(place);
        let other = Self::get_state(rhs);

        Self::merge_states_inner(state, &other.columns);
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = Self::get_state(place);

        if state.columns.is_empty() || state.columns[0].len() == 0 {
            return Ok(());
        }
        let num_rows = state.columns[0].len();

        let mut block = if read_only {
            DataBlock::new(
                state
                    .columns
                    .iter()
                    .map(|builder| builder.clone().build().into())
                    .collect(),
                num_rows,
            )
        } else {
            DataBlock::new(
                state
                    .columns
                    .drain(..)
                    .map(|builder| builder.build().into())
                    .collect(),
                num_rows,
            )
        };

        let mut not_arg_indexes = HashSet::with_capacity(self.sort_descs.len());
        let mut sort_descs = Vec::with_capacity(self.sort_descs.len());

        for desc in self.sort_descs.iter() {
            if !desc.is_reuse_index {
                not_arg_indexes.insert(desc.index);
            }

            sort_descs.push(SortColumnDescription {
                offset: desc.index,
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            });
        }
        block = DataBlock::sort(&block, &sort_descs, None)?;

        let inner_place = place.remove_first_loc();
        self.inner.init_state(inner_place);

        let args = (0..block.num_columns())
            .filter(|i| !not_arg_indexes.contains(i))
            .collect_vec();

        self.inner.accumulate(
            inner_place,
            ProjectedBlock::project(&args, &block),
            None,
            num_rows,
        )?;
        self.inner.merge_result(inner_place, false, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = Self::get_state(place);
        std::ptr::drop_in_place(state);

        if self.inner.need_manual_drop_state() {
            self.inner.drop_state(place.remove_first_loc());
        }
    }

    fn get_if_condition(&self, columns: ProjectedBlock) -> Option<Bitmap> {
        self.inner.get_if_condition(columns)
    }
}

impl AggregateFunctionSortAdaptor {
    pub fn create(
        inner: AggregateFunctionRef,
        sort_descs: Vec<AggregateFunctionSortDesc>,
    ) -> Result<AggregateFunctionRef> {
        if sort_descs.is_empty() {
            return Ok(inner);
        }
        Ok(Arc::new(AggregateFunctionSortAdaptor {
            name: format!("SortCombinator({})", inner.name()),
            inner,
            sort_descs,
        }))
    }

    fn get_state(place: AggrState) -> &mut SortAggState {
        place
            .addr
            .next(place.loc[0].into_custom().unwrap().1)
            .get::<SortAggState>()
    }

    fn set_state(place: AggrState, state: SortAggState) {
        place
            .addr
            .next(place.loc[0].into_custom().unwrap().1)
            .write_state(state);
    }

    fn init_columns(block: ProjectedBlock, state: &mut SortAggState, num_rows: Option<usize>) {
        if !state.columns.is_empty() {
            return;
        }
        state.columns = block
            .iter()
            .map(|entry| {
                ColumnBuilder::with_capacity(&entry.data_type(), num_rows.unwrap_or_default())
            })
            .collect();
    }

    fn merge_states_inner(state: &mut SortAggState, other: &[ColumnBuilder]) {
        if state.columns.is_empty() {
            state.columns = other.to_vec();
        } else {
            for (l, r) in state.columns.iter_mut().zip(other) {
                l.append_column(&r.clone().build())
            }
        }
    }
}

impl Display for AggregateFunctionSortAdaptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}
