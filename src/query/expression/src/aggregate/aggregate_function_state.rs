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

use databend_common_exception::Result;
use enum_as_inner::EnumAsInner;

use super::AggregateFunctionRef;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::DataType;
use crate::ColumnBuilder;

#[derive(Clone, Copy, Debug)]
pub struct StateAddr(*mut u8);

unsafe impl Send for StateAddr {}

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

pub fn get_states_layout(funcs: &[AggregateFunctionRef]) -> Result<StatesLayout> {
    let mut registry = AggrStateRegistry::default();
    let mut serialize_type = Vec::with_capacity(funcs.len());
    for func in funcs {
        func.register_state(&mut registry);
        registry.commit();
        serialize_type.push(StateSerdeType(func.serialize_type().into()));
    }

    let AggrStateRegistry { states, offsets } = registry;

    let (layout, locs) = sort_states(states);

    let states_loc = offsets
        .windows(2)
        .map(|w| locs[w[0]..w[1]].to_vec().into_boxed_slice())
        .collect::<Vec<_>>();

    Ok(StatesLayout {
        layout,
        states_loc,
        serialize_type,
    })
}

fn sort_states(states: Vec<AggrStateType>) -> (Layout, Vec<AggrStateLoc>) {
    let mut states = states
        .iter()
        .enumerate()
        .map(|(idx, state)| {
            let layout = match state {
                AggrStateType::Bool => (1, 1),
                AggrStateType::Custom(layout) => (layout.align(), layout.pad_to_align().size()),
            };
            (idx, state, layout)
        })
        .collect::<Vec<_>>();

    states.sort_by_key(|(_, _, (align, _))| std::cmp::Reverse(*align));

    let mut locs = vec![AggrStateLoc::Bool(0, 0); states.len()];
    let mut acc = 0;
    let mut max_align = 0;
    for (idx, state, (align, size)) in states {
        max_align = max_align.max(align);
        let offset = acc;
        acc += size;
        locs[idx] = match state {
            AggrStateType::Bool => AggrStateLoc::Bool(idx, offset),
            AggrStateType::Custom(_) => AggrStateLoc::Custom(idx, offset),
        };
    }

    let layout = Layout::from_size_align(acc, max_align).unwrap();

    (layout, locs)
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
    pub(super) serialize_type: Vec<StateSerdeType>,
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
        debug_assert_eq!(self.loc.len(), 1);
        self.addr
            .next(self.loc[0].into_custom().unwrap().1)
            .get::<T>()
    }

    pub fn write<T, F>(&self, f: F)
    where
        F: FnOnce() -> T,
        T: Send + 'static,
    {
        debug_assert_eq!(self.loc.len(), 1);
        self.addr
            .next(self.loc[0].into_custom().unwrap().1)
            .write(f);
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

pub struct AggrStateRegistry {
    states: Vec<AggrStateType>,
    offsets: Vec<usize>,
}

impl AggrStateRegistry {
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

impl Default for AggrStateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AggrStateType {
    Bool,
    Custom(Layout),
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    use super::*;

    prop_compose! {
        fn arb_state_type()(size in 1..100_usize, align in 0..5_u8) -> AggrStateType {
            let layout = Layout::from_size_align(size, 1 << align).unwrap();
            AggrStateType::Custom(layout)
        }
    }

    #[test]
    fn test_sort_states() {
        let mut runner = TestRunner::default();
        let input_s = prop::collection::vec(arb_state_type(), 1..20);

        for _ in 0..100 {
            let input = input_s.new_tree(&mut runner).unwrap().current();
            run_sort_states(input);
        }
    }

    fn check_offset(layout: &Layout, offset: usize) -> bool {
        let align = layout.align();
        offset & (align - 1) == 0
    }

    fn run_sort_states(input: Vec<AggrStateType>) {
        let (layout, locs) = sort_states(input.clone());

        let is_aligned = input
            .iter()
            .zip(locs.iter())
            .all(|(state, loc)| match state {
                AggrStateType::Custom(layout) => check_offset(layout, loc.offset()),
                _ => unreachable!(),
            });

        assert!(is_aligned, "states are not aligned, input: {input:?}");

        let size = layout.size();
        let mut memory = vec![false; size];
        for (state, loc) in input.iter().zip(locs.iter()) {
            match state {
                AggrStateType::Custom(layout) => {
                    let start = loc.offset();
                    let end = start + layout.size();
                    for i in start..end {
                        assert!(!memory[i], "layout is overlap, input: {input:?}");
                        memory[i] = true;
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
