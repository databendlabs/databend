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
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;

use super::StateAddr;

pub type AggregateFunctionRef = Arc<dyn AggregateFunction>;

/// AggregateFunction
/// In AggregateFunction, all datablock columns are not ConstantColumn, we take the column as Full columns
pub trait AggregateFunction: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;

    fn init_state(&self, place: StateAddr);

    fn is_state(&self) -> bool {
        false
    }

    fn state_layout(&self) -> Layout;

    // accumulate is to accumulate the arrays in batch mode
    // common used when there is no group by for aggregate function
    fn accumulate(
        &self,
        _place: StateAddr,
        _columns: &[Column],
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()>;

    // used when we need to calculate with group keys
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        for (row, place) in places.iter().enumerate() {
            self.accumulate_row(place.next(offset), columns, row)?;
        }
        Ok(())
    }

    // Used in aggregate_null_adaptor
    fn accumulate_row(&self, _place: StateAddr, _columns: &[Column], _row: usize) -> Result<()>;

    // serialize  the state into binary array
    fn batch_serialize(
        &self,
        places: &[StateAddr],
        offset: usize,
        builder: &mut StringColumnBuilder,
    ) -> Result<()> {
        for place in places {
            self.serialize(place.next(offset), &mut builder.data)?;
            builder.commit_row();
        }
        Ok(())
    }

    fn serialize(&self, _place: StateAddr, _writer: &mut Vec<u8>) -> Result<()>;

    fn serialize_size_per_row(&self) -> Option<usize> {
        None
    }

    fn merge(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()>;

    fn batch_merge(&self, places: &[StateAddr], offset: usize, column: &Column) -> Result<()> {
        let c = column.as_string().unwrap();
        for (place, mut data) in places.iter().zip(c.iter()) {
            self.merge(place.next(offset), &mut data)?;
        }

        Ok(())
    }

    fn batch_merge_single(&self, place: StateAddr, column: &Column) -> Result<()> {
        let c = column.as_string().unwrap();

        for mut data in c.iter() {
            self.merge(place, &mut data)?;
        }
        Ok(())
    }

    fn merge_states(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()>;

    fn batch_merge_result(&self, places: &[StateAddr], builder: &mut ColumnBuilder) -> Result<()> {
        for place in places {
            self.merge_result(*place, builder)?;
        }
        Ok(())
    }
    // TODO append the value into the column builder
    fn merge_result(&self, _place: StateAddr, _builder: &mut ColumnBuilder) -> Result<()>;

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
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<AggregateFunctionRef>> {
        Ok(None)
    }

    fn get_if_condition(&self, _columns: &[Column]) -> Option<Bitmap> {
        None
    }

    // some features
    fn convert_const_to_full(&self) -> bool {
        true
    }
}
