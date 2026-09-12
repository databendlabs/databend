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
use std::ptr::NonNull;

use databend_common_base::hints::assume;
use enum_as_inner::EnumAsInner;

use crate::ColumnBuilder;
use crate::types::DataType;
use crate::types::binary::BinaryColumnBuilder;

#[derive(Clone, Copy, Debug)]
pub struct StateAddr(*mut u8);

pub type StateAddrs = Vec<StateAddr>;

impl StateAddr {
    #[inline]
    pub fn null() -> StateAddr {
        Self(std::ptr::null_mut())
    }

    #[inline]
    pub fn get<'a, T>(&self) -> &'a mut T
    where T: Send + 'static {
        unsafe { &mut *self.0.cast::<T>() }
    }

    #[inline]
    pub fn get_ref<'a, T>(&self) -> &'a T
    where T: Send + 'static {
        unsafe { &*self.0.cast::<T>() }
    }

    #[inline]
    #[must_use]
    pub fn next(&self, offset: usize) -> Self {
        unsafe { Self(self.0.add(offset)) }
    }

    #[inline]
    pub fn write<T, F>(&self, f: F)
    where
        F: FnOnce() -> T,
        T: Send + 'static,
    {
        unsafe {
            let ptr = self.0.cast::<T>();
            std::ptr::write(ptr, f());
        }
    }

    #[inline]
    pub fn write_state<T>(&self, state: T)
    where T: Send + 'static {
        unsafe {
            let ptr = self.0.cast::<T>();
            std::ptr::write(ptr, state);
        }
    }
}

impl From<NonNull<u8>> for StateAddr {
    fn from(s: NonNull<u8>) -> Self {
        Self(s.as_ptr())
    }
}

impl From<*mut u8> for StateAddr {
    fn from(s: *mut u8) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, Copy, EnumAsInner)]
pub enum AggrStateLoc {
    Bool(usize, usize),   // index, offset
    Custom(usize, usize), // index, offset
}

impl AggrStateLoc {
    pub fn offset(&self) -> usize {
        match self {
            AggrStateLoc::Bool(_, offset) => *offset,
            AggrStateLoc::Custom(_, offset) => *offset,
        }
    }

    pub fn index(&self) -> usize {
        match self {
            AggrStateLoc::Bool(idx, _) => *idx,
            AggrStateLoc::Custom(idx, _) => *idx,
        }
    }
}

#[derive(Debug, Clone)]
pub enum StateSerdeItem {
    DataType(DataType),
    Binary(Option<usize>),
}

impl From<DataType> for StateSerdeItem {
    fn from(value: DataType) -> Self {
        Self::DataType(value)
    }
}

#[derive(Debug, Clone)]
pub struct StateSerdeType(Box<[StateSerdeItem]>);

impl StateSerdeType {
    pub fn new(items: impl Into<Box<[StateSerdeItem]>>) -> Self {
        StateSerdeType(items.into())
    }

    pub fn data_type(&self) -> DataType {
        DataType::Tuple(
            self.0
                .iter()
                .map(|item| match item {
                    StateSerdeItem::DataType(data_type) => data_type.clone(),
                    StateSerdeItem::Binary(_) => DataType::Binary,
                })
                .collect(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct StatesLayout {
    pub layout: Layout,
    pub states_loc: Vec<Box<[AggrStateLoc]>>,
    pub serialize_type: Vec<StateSerdeType>,
}

impl StatesLayout {
    pub fn num_aggr_func(&self) -> usize {
        self.states_loc.len()
    }

    pub fn serialize_builders(&self, num_rows: usize) -> Vec<ColumnBuilder> {
        self.serialize_type
            .iter()
            .map(|serde_type| {
                let builder = serde_type
                    .0
                    .iter()
                    .map(|item| match item {
                        StateSerdeItem::DataType(data_type) => {
                            ColumnBuilder::with_capacity(data_type, num_rows)
                        }
                        StateSerdeItem::Binary(size) => {
                            ColumnBuilder::Binary(BinaryColumnBuilder::with_capacity(
                                num_rows,
                                num_rows * size.unwrap_or(0),
                            ))
                        }
                    })
                    .collect();
                ColumnBuilder::Tuple(builder)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AggrState<'a> {
    pub addr: StateAddr,
    pub loc: &'a [AggrStateLoc],
}

impl<'a> AggrState<'a> {
    pub fn new(addr: StateAddr, loc: &'a [AggrStateLoc]) -> Self {
        Self { addr, loc }
    }

    pub fn get<'b, T>(&self) -> &'b mut T
    where T: Send + 'static {
        assume(self.loc.len() == 1);
        debug_assert!(self.loc[0].is_custom());
        self.addr.next(self.loc[0].offset()).get::<T>()
    }

    pub fn get_ref<'b, T>(&self) -> &'b T
    where T: Send + 'static {
        assume(self.loc.len() == 1);
        debug_assert!(self.loc[0].is_custom());
        self.addr.next(self.loc[0].offset()).get_ref::<T>()
    }

    pub fn write<T, F>(&self, f: F)
    where
        F: FnOnce() -> T,
        T: Send + 'static,
    {
        assume(self.loc.len() == 1);
        debug_assert!(self.loc[0].is_custom());
        self.addr.next(self.loc[0].offset()).write(f);
    }

    pub fn remove_last_loc(&self) -> Self {
        debug_assert!(self.loc.len() >= 2);
        Self {
            addr: self.addr,
            loc: &self.loc[..self.loc.len() - 1],
        }
    }

    pub fn remove_first_loc(&self) -> Self {
        debug_assert!(self.loc.len() >= 2);
        Self {
            addr: self.addr,
            loc: &self.loc[1..],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AggrStateType {
    Bool,
    Custom(Layout),
}
