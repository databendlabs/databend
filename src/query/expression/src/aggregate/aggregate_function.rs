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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;

use super::AggrStateLoc;
use super::StateAddr;
use crate::types::BinaryColumn;
use crate::types::DataType;
use crate::Column;
use crate::ColumnBuilder;
use crate::InputColumns;
use crate::Scalar;

pub type AggregateFunctionRef = Arc<dyn AggregateFunction>;

/// AggregateFunction
/// In AggregateFunction, all datablock columns are not ConstantColumn, we take the column as Full columns
pub trait AggregateFunction: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;

    fn init_state(&self, place: &AggrState);

    fn state_layout(&self) -> Layout;

    fn register_state(&self, register: &mut AggrStateRegister) {
        register.register(AggrStateType::Custom(self.state_layout()));
    }

    // accumulate is to accumulate the arrays in batch mode
    // common used when there is no group by for aggregate function
    fn accumulate(
        &self,
        place: &AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()>;

    // used when we need to calculate with group keys
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        for (row, place) in places.iter().enumerate() {
            self.accumulate_row(&AggrState::with_loc(*place, loc), columns, row)?;
        }
        Ok(())
    }

    // Used in aggregate_null_adaptor
    fn accumulate_row(&self, place: &AggrState, columns: InputColumns, row: usize) -> Result<()>;

    fn serialize(&self, place: &AggrState, writer: &mut Vec<u8>) -> Result<()>;

    fn serialize_size_per_row(&self) -> Option<usize> {
        None
    }

    fn merge(&self, place: &AggrState, reader: &mut &[u8]) -> Result<()>;

    /// Batch merge and deserialize the state from binary array
    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: Box<[AggrStateLoc]>,
        state: &BinaryColumn,
    ) -> Result<()> {
        for (place, mut data) in places.iter().zip(state.iter()) {
            self.merge(&AggrState::with_loc(*place, &loc), &mut data)?;
        }

        Ok(())
    }

    fn batch_merge_single(&self, place: &AggrState, state: &Column) -> Result<()> {
        let c = state.as_binary().unwrap();
        for mut data in c.iter() {
            self.merge(place, &mut data)?;
        }
        Ok(())
    }

    fn batch_merge_states(
        &self,
        places: &[StateAddr],
        rhses: &[StateAddr],
        loc: Box<[AggrStateLoc]>,
    ) -> Result<()> {
        for (place, rhs) in places.iter().zip(rhses.iter()) {
            self.merge_states(
                &AggrState::with_loc(*place, &loc),
                &AggrState::with_loc(*rhs, &loc),
            )?;
        }
        Ok(())
    }

    fn merge_states(&self, place: &AggrState, rhs: &AggrState) -> Result<()>;

    fn batch_merge_result(
        &self,
        places: &[StateAddr],
        loc: Box<[AggrStateLoc]>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        for place in places {
            self.merge_result(&AggrState::with_loc(*place, &loc), builder)?;
        }
        Ok(())
    }

    fn merge_result(&self, place: &AggrState, builder: &mut ColumnBuilder) -> Result<()>;

    // std::mem::needs_drop::<State>
    // if true will call drop_state
    fn need_manual_drop_state(&self) -> bool {
        false
    }

    /// # Safety
    /// The caller must ensure that the [`_place`] has defined memory.
    unsafe fn drop_state(&self, _place: &AggrState) {}

    fn get_own_null_adaptor(
        &self,
        _nested_function: AggregateFunctionRef,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
    ) -> Result<Option<AggregateFunctionRef>> {
        Ok(None)
    }

    fn get_if_condition(&self, _columns: InputColumns) -> Option<Bitmap> {
        None
    }

    // some features
    fn convert_const_to_full(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct AggrState<'a> {
    pub addr: StateAddr,
    loc: &'a [AggrStateLoc],
}

impl<'a> AggrState<'a> {
    pub fn with_loc(addr: StateAddr, loc: &'a [AggrStateLoc]) -> Self {
        Self { addr, loc }
    }

    pub fn get<'b, T>(&self) -> &'b mut T {
        self.addr
            .next(self.loc[0].into_custom().unwrap().1)
            .get::<T>()
    }

    pub fn write<T, F>(&self, f: F)
    where F: FnOnce() -> T {
        self.addr
            .next(self.loc[0].into_custom().unwrap().1)
            .write(f);
    }

    pub fn write_state<T>(&self, state: T) {
        self.addr
            .next(self.loc[0].into_custom().unwrap().1)
            .write_state(state);
    }

    pub fn loc(&self) -> &[AggrStateLoc] {
        self.loc
    }

    pub fn remove_last_loc(&self) -> Self {
        assert!(self.loc.len() >= 2);
        Self {
            addr: self.addr,
            loc: &self.loc[..self.loc.len() - 1],
        }
    }
}

pub struct AggrStateRegister {
    pub(super) states: Vec<AggrStateType>,
    pub(super) offsets: Vec<usize>,
}

impl AggrStateRegister {
    pub fn new() -> Self {
        Self {
            states: vec![],
            offsets: vec![0],
        }
    }

    pub fn register(&mut self, state: AggrStateType) {
        self.states.push(state);
    }

    pub fn commit(&mut self) {
        self.offsets.push(self.states.len());
    }

    pub fn states(&self) -> &[AggrStateType] {
        &self.states
    }
}

impl Default for AggrStateRegister {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AggrStateType {
    Bool,
    Custom(Layout),
}
