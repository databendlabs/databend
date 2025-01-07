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

use super::AggrStateRegister;
use super::AggrStateType;
use super::AggregateFunctionRef;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::ColumnBuilder;

#[derive(Clone, Copy, Debug)]
pub struct StateAddr {
    addr: usize,
}

pub type StateAddrs = Vec<StateAddr>;

impl StateAddr {
    #[inline]
    pub fn new(addr: usize) -> StateAddr {
        Self { addr }
    }

    #[inline]
    pub fn get<'a, T>(&self) -> &'a mut T {
        unsafe { &mut *(self.addr as *mut T) }
    }

    #[inline]
    pub fn addr(&self) -> usize {
        self.addr
    }

    /// # Safety
    /// ptr must ensure point to valid memory
    #[inline]
    pub unsafe fn from_ptr(ptr: *mut u8) -> Self {
        Self { addr: ptr as usize }
    }

    #[inline]
    #[must_use]
    pub fn next(&self, offset: usize) -> Self {
        Self {
            addr: self.addr + offset,
        }
    }

    #[inline]
    #[must_use]
    pub fn prev(&self, offset: usize) -> Self {
        Self {
            addr: self.addr.wrapping_sub(offset),
        }
    }

    #[inline]
    pub fn write<T, F>(&self, f: F)
    where F: FnOnce() -> T {
        unsafe {
            let ptr = self.addr as *mut T;
            std::ptr::write(ptr, f());
        }
    }

    #[inline]
    pub fn write_state<T>(&self, state: T) {
        unsafe {
            let ptr = self.addr as *mut T;
            std::ptr::write(ptr, state);
        }
    }
}

impl From<NonNull<u8>> for StateAddr {
    fn from(s: NonNull<u8>) -> Self {
        Self {
            addr: s.as_ptr() as usize,
        }
    }
}

impl From<usize> for StateAddr {
    fn from(addr: usize) -> Self {
        Self { addr }
    }
}

impl From<StateAddr> for NonNull<u8> {
    fn from(s: StateAddr) -> Self {
        unsafe { NonNull::new_unchecked(s.addr as *mut u8) }
    }
}

impl From<StateAddr> for usize {
    fn from(s: StateAddr) -> Self {
        s.addr
    }
}

pub fn get_state_layout(funcs: &[AggregateFunctionRef]) -> Result<StatesLayout> {
    let mut register = AggrStateRegister::new();
    for func in funcs {
        func.register_state(&mut register);
        register.commit();
    }

    let AggrStateRegister { states, offsets } = register;

    let (layout, locs) = sort_states(states);

    let loc = offsets
        .windows(2)
        .map(|w| locs[w[0]..w[1]].to_vec().into_boxed_slice())
        .collect::<Vec<_>>();

    Ok(StatesLayout { layout, loc })
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
    Bool(usize, usize),   // index
    Custom(usize, usize), // index,offset
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
pub struct StatesLayout {
    pub layout: Layout,
    pub loc: Vec<Box<[AggrStateLoc]>>,
}

impl StatesLayout {
    pub fn serialize_builders(&self, num_rows: usize) -> Vec<ColumnBuilder> {
        self.loc
            .iter()
            .flatten()
            .map(|loc| match loc {
                AggrStateLoc::Bool(_, _) => {
                    ColumnBuilder::Boolean(BooleanType::create_builder(num_rows, &[]))
                }
                AggrStateLoc::Custom(_, _) => {
                    ColumnBuilder::Binary(BinaryColumnBuilder::with_capacity(num_rows, 0))
                }
            })
            .collect()
    }

    pub fn num_states(&self) -> usize {
        self.loc.iter().flatten().count()
    }

    pub fn states_ranges(&self) -> Vec<std::ops::Range<usize>> {
        let mut ranges = Vec::with_capacity(self.loc.len());
        let mut acc = 0;
        for n in self.loc.iter().map(|loc| loc.len()) {
            let start = acc;
            acc += n;
            ranges.push(start..acc);
        }
        ranges
    }
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
