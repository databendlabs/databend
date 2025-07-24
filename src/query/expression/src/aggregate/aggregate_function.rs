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
use crate::types::AnyPairType;
use crate::types::AnyQuaternaryType;
use crate::types::AnyTernaryType;
use crate::types::AnyType;
use crate::types::AnyUnaryType;
use crate::types::DataType;
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::ScalarRef;
use crate::StateSerdeItem;
use crate::StateSerdeType;

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

    fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()>;

    fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()>;

    /// Batch deserialize the state and merge
    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        match state.data_type().as_tuple().unwrap().len() {
            1 => {
                let view = state.downcast::<AnyUnaryType>().unwrap();
                let iter = places.iter().zip(view.iter());
                if let Some(filter) = filter {
                    for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v))
                    {
                        self.merge(AggrState::new(*place, loc), &[data])?;
                    }
                } else {
                    for (place, data) in iter {
                        self.merge(AggrState::new(*place, loc), &[data])?;
                    }
                }
            }
            2 => {
                let view = state.downcast::<AnyPairType>().unwrap();
                let iter = places.iter().zip(view.iter());
                if let Some(filter) = filter {
                    for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v))
                    {
                        self.merge(AggrState::new(*place, loc), &[data.0, data.1])?;
                    }
                } else {
                    for (place, data) in iter {
                        self.merge(AggrState::new(*place, loc), &[data.0, data.1])?;
                    }
                }
            }
            3 => {
                let view = state.downcast::<AnyTernaryType>().unwrap();
                let iter = places.iter().zip(view.iter());
                if let Some(filter) = filter {
                    for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v))
                    {
                        self.merge(AggrState::new(*place, loc), &[data.0, data.1, data.2])?;
                    }
                } else {
                    for (place, data) in iter {
                        self.merge(AggrState::new(*place, loc), &[data.0, data.1, data.2])?;
                    }
                }
            }
            4 => {
                let view = state.downcast::<AnyQuaternaryType>().unwrap();
                let iter = places.iter().zip(view.iter());
                if let Some(filter) = filter {
                    for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v))
                    {
                        self.merge(AggrState::new(*place, loc), &[
                            data.0, data.1, data.2, data.3,
                        ])?;
                    }
                } else {
                    for (place, data) in iter {
                        self.merge(AggrState::new(*place, loc), &[
                            data.0, data.1, data.2, data.3,
                        ])?;
                    }
                }
            }
            _ => {
                let view = state.downcast::<AnyType>().unwrap();
                let iter = places.iter().zip(view.iter());
                if let Some(filter) = filter {
                    for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v))
                    {
                        self.merge(
                            AggrState::new(*place, loc),
                            data.as_tuple().unwrap().as_slice(),
                        )?;
                    }
                } else {
                    for (place, data) in iter {
                        self.merge(
                            AggrState::new(*place, loc),
                            data.as_tuple().unwrap().as_slice(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn batch_merge_single(&self, place: AggrState, state: &BlockEntry) -> Result<()> {
        match state.data_type().as_tuple().unwrap().len() {
            1 => {
                let view = state.downcast::<AnyUnaryType>().unwrap();
                for data in view.iter() {
                    self.merge(place, &[data])?;
                }
            }
            2 => {
                let view = state.downcast::<AnyPairType>().unwrap();
                for data in view.iter() {
                    self.merge(place, &[data.0, data.1])?;
                }
            }
            3 => {
                let view = state.downcast::<AnyTernaryType>().unwrap();
                for data in view.iter() {
                    self.merge(place, &[data.0, data.1, data.2])?;
                }
            }
            4 => {
                let view = state.downcast::<AnyQuaternaryType>().unwrap();
                for data in view.iter() {
                    self.merge(place, &[data.0, data.1, data.2, data.3])?;
                }
            }
            _ => {
                let state = state.to_column();
                for data in state.iter() {
                    self.merge(place, data.as_tuple().unwrap().as_slice())?;
                }
            }
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
