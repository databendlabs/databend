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
use databend_common_expression::types::DataType;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::StateSerdeItem;
use itertools::Itertools;

use crate::aggregates::AggregateFunctionSortDesc;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct SortAggState {
    columns: Vec<Vec<Scalar>>,
    data_types: Vec<DataType>,
}

pub struct AggregateFunctionSortAdaptor {
    inner: AggregateFunctionRef,
    sort_descs: Vec<AggregateFunctionSortDesc>,
}

impl AggregateFunction for AggregateFunctionSortAdaptor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self) -> Result<DataType> {
        self.inner.return_type()
    }

    #[inline]
    fn init_state(&self, place: AggrState) {
        Self::set_state(place, SortAggState {
            columns: vec![],
            data_types: vec![],
        });
        self.inner.init_state(place.remove_first_loc());
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<SortAggState>()));
        self.inner.register_state(registry);
    }

    fn accumulate(
        &self,
        place: AggrState,
        entries: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = Self::get_state(place);
        Self::init_columns(entries, state, Some(input_rows));

        match validity {
            Some(validity) => {
                for (i, entry) in entries.iter().enumerate() {
                    entry
                        .downcast::<AnyType>()
                        .unwrap()
                        .iter()
                        .zip(validity.iter())
                        .for_each(|(v, b)| {
                            if b {
                                state.columns[i].push(v.to_owned());
                            }
                        });
                }
            }
            None => {
                for (i, entry) in entries.iter().enumerate() {
                    entry.downcast::<AnyType>().unwrap().iter().for_each(|v| {
                        state.columns[i].push(v.to_owned());
                    });
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let state = Self::get_state(place);
        Self::init_columns(columns, state, None);

        for (i, column) in columns.iter().enumerate() {
            if let Some(v) = column.index(row) {
                state.columns[i].push(v.to_owned())
            }
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn serialize_binary(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        let state = Self::get_state(place);
        Ok(state.serialize(writer)?)
    }

    fn merge_binary(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let state = Self::get_state(place);
        let rhs = SortAggState::deserialize(reader)?;

        Self::merge_states_inner(state, &rhs);
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = Self::get_state(place);
        let other = Self::get_state(rhs);

        Self::merge_states_inner(state, other);
        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state = Self::get_state(place);
        let inner_place = place.remove_first_loc();

        if state.columns.is_empty() || state.columns[0].is_empty() {
            return Ok(());
        }
        let num_rows = state.columns[0].len();

        let mut block = DataBlock::new(
            state
                .columns
                .iter()
                .zip(state.data_types.iter())
                .map(|(values, data_type)| {
                    let mut builder = ColumnBuilder::with_capacity(data_type, values.len());
                    for value in values {
                        builder.push(value.as_ref());
                    }
                    builder.build().into()
                })
                .collect_vec(),
            num_rows,
        );
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

        let args = (0..block.num_columns())
            .filter(|i| !not_arg_indexes.contains(i))
            .collect_vec();

        self.inner.init_state(inner_place);
        self.inner.accumulate(
            inner_place,
            ProjectedBlock::project(&args, &block),
            None,
            num_rows,
        )?;
        self.inner.merge_result(inner_place, builder)
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
        Ok(Arc::new(AggregateFunctionSortAdaptor { inner, sort_descs }))
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

    fn init_columns(columns: ProjectedBlock, state: &mut SortAggState, num_rows: Option<usize>) {
        if state.columns.is_empty() && state.data_types.is_empty() {
            state.columns = Vec::with_capacity(columns.len());
            state.data_types = Vec::with_capacity(columns.len());

            for column in columns.iter() {
                state.data_types.push(column.data_type());
                state.columns.push(
                    num_rows
                        .map(|num| Vec::with_capacity(num))
                        .unwrap_or_default(),
                );
            }
        }
    }

    fn merge_states_inner(state: &mut SortAggState, other: &SortAggState) {
        if state.columns.is_empty() && state.data_types.is_empty() {
            state.columns = other.columns.clone();
            state.data_types = other.data_types.clone();
        } else {
            state
                .columns
                .iter_mut()
                .zip(other.columns.iter())
                .for_each(|(l, r)| l.extend_from_slice(r));
        }
    }
}

impl Display for AggregateFunctionSortAdaptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
