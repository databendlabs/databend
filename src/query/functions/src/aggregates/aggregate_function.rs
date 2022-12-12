// Copyright 2021 Datafuse Labs.
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
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_base::runtime::ThreadPool;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::StateAddr;

pub type AggregateFunctionRef = Arc<dyn AggregateFunction>;

/// AggregateFunction
/// In AggregateFunction, all datablock columns are not ConstantColumn, we take the column as Full columns
pub trait AggregateFunction: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataTypeImpl>;

    fn init_state(&self, place: StateAddr);
    fn state_layout(&self) -> Layout;

    // accumulate is to accumulate the arrays in batch mode
    // common used when there is no group by for aggregate function
    fn accumulate(
        &self,
        _place: StateAddr,
        _columns: &[ColumnRef],
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()>;

    // used when we need to calculate with group keys
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        for (row, place) in places.iter().enumerate() {
            self.accumulate_row(place.next(offset), columns, row)?;
        }
        Ok(())
    }

    // Used in aggregate_null_adaptor
    fn accumulate_row(&self, _place: StateAddr, _columns: &[ColumnRef], _row: usize) -> Result<()>;

    // serialize  the state into binary array
    fn serialize(&self, _place: StateAddr, _writer: &mut Vec<u8>) -> Result<()>;

    fn deserialize(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()>;

    fn merge(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()>;

    fn support_merge_parallel(&self) -> bool {
        false
    }

    fn merge_parallel(
        &self,
        _pool: &mut ThreadPool,
        _place: StateAddr,
        _rhs: StateAddr,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "merge_parallel is not implemented for {}",
            self.name()
        )))
    }

    fn batch_merge_result(
        &self,
        places: Vec<StateAddr>,
        array: &mut dyn MutableColumn,
    ) -> Result<()> {
        for place in places {
            self.merge_result(place, array)?;
        }
        Ok(())
    }

    fn merge_result(&self, _place: StateAddr, array: &mut dyn MutableColumn) -> Result<()>;

    // std::mem::needs_drop::<State>
    // if true will call drop_state
    fn need_manual_drop_state(&self) -> bool {
        false
    }

    /// # Safety
    /// The caller must ensure that the [`_place`] has defined memory.
    unsafe fn drop_state(&self, _place: StateAddr) {}

    fn get_own_null_adaptor(
        &self,
        _nested_function: AggregateFunctionRef,
        _params: Vec<DataValue>,
        _arguments: Vec<DataField>,
    ) -> Result<Option<AggregateFunctionRef>> {
        Ok(None)
    }

    fn get_if_condition(&self, _columns: &[ColumnRef]) -> Option<Bitmap> {
        None
    }

    // some features
    fn convert_const_to_full(&self) -> bool {
        true
    }
}
