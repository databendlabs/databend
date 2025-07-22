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

use std::fmt;
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;

use super::AggrState;
use super::AggrStateLoc;
use super::AggrStateRegistry;
use super::StateAddr;
use crate::types::DataType;
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::ScalarRef;
use crate::StateSerdeItem;

pub type AggregateFunctionRef = Arc<dyn AggregateFunction>;

/// AggregateFunction
/// In AggregateFunction, all datablock columns are not ConstantColumn, we take the column as Full columns
pub trait AggregateFunction: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;

    fn init_state(&self, place: AggrState);

    fn register_state(&self, registry: &mut AggrStateRegistry);

    // accumulate is to accumulate the arrays in batch mode
    // common used when there is no group by for aggregate function
    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()>;

    // used when we need to calculate with group keys
    fn accumulate_keys(
        &self,
        addrs: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        for (row, addr) in addrs.iter().enumerate() {
            self.accumulate_row(AggrState::new(*addr, loc), columns, row)?;
        }
        Ok(())
    }

    // Used in aggregate_null_adaptor
    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()>;

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(self.serialize_size_per_row())]
    }

    fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        self.serialize_binary(place, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn serialize_binary(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()>;

    fn serialize_size_per_row(&self) -> Option<usize> {
        None
    }

    fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()> {
        let mut binary = *data[0].as_binary().unwrap();
        self.merge_binary(place, &mut binary)
    }

    fn merge_binary(&self, place: AggrState, reader: &mut &[u8]) -> Result<()>;

    /// Batch merge and deserialize the state from binary array
    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
    ) -> Result<()> {
        let column = state.to_column();
        for (place, data) in places.iter().zip(column.iter()) {
            self.merge(
                AggrState::new(*place, loc),
                data.as_tuple().unwrap().as_slice(),
            )?;
        }

        Ok(())
    }

    fn batch_merge_single(&self, place: AggrState, state: &BlockEntry) -> Result<()> {
        let column = state.to_column();
        for data in column.iter() {
            self.merge(place, data.as_tuple().unwrap().as_slice())?;
        }
        Ok(())
    }

    fn batch_merge_states(
        &self,
        places: &[StateAddr],
        rhses: &[StateAddr],
        loc: &[AggrStateLoc],
    ) -> Result<()> {
        for (place, rhs) in places.iter().zip(rhses.iter()) {
            self.merge_states(AggrState::new(*place, loc), AggrState::new(*rhs, loc))?;
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()>;

    fn batch_merge_result(
        &self,
        places: &[StateAddr],
        loc: Box<[AggrStateLoc]>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        for place in places {
            self.merge_result(AggrState::new(*place, &loc), builder)?;
        }
        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()>;

    // std::mem::needs_drop::<State>
    // if true will call drop_state
    fn need_manual_drop_state(&self) -> bool {
        false
    }

    /// # Safety
    /// The caller must ensure that the [`_place`] has defined memory.
    unsafe fn drop_state(&self, _place: AggrState) {}

    fn get_own_null_adaptor(
        &self,
        _nested_function: AggregateFunctionRef,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<AggregateFunctionRef>> {
        Ok(None)
    }

    fn get_if_condition(&self, _columns: ProjectedBlock) -> Option<Bitmap> {
        None
    }
}
