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
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::StateSerdeItem;
use crate::StateSerdeType;
use crate::types::DataType;

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

    fn serialize_type(&self) -> Vec<StateSerdeItem>;

    fn serialize_data_type(&self) -> DataType {
        let serde_type = StateSerdeType::new(self.serialize_type());
        serde_type.data_type()
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()>;

    /// Batch deserialize the state and merge
    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()>;

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
            self.merge_result(AggrState::new(*place, &loc), false, builder)?;
        }
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()>;

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
