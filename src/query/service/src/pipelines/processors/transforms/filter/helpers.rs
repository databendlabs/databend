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

use core::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq)]

pub enum SelectStrategy {
    True,
    False,
    All,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum SelectOp {
    Equal,
    NotEqual,
    // Greater ">"
    Gt,
    // Less "<"
    Lt,
    // Greater or equal ">="
    Gte,
    // Less or equal "<="
    Lte,
}

impl SelectOp {
    pub fn try_from_func_name(name: &str) -> Option<Self> {
        match name {
            "eq" => Some(Self::Equal),
            "noteq" => Some(Self::NotEqual),
            "gt" => Some(Self::Gt),
            "lt" => Some(Self::Lt),
            "gte" => Some(Self::Gte),
            "lte" => Some(Self::Lte),
            _ => None,
        }
    }

    pub fn reverse(&self) -> Self {
        match &self {
            SelectOp::Equal => SelectOp::Equal,
            SelectOp::NotEqual => SelectOp::NotEqual,
            SelectOp::Gt => SelectOp::Lt,
            SelectOp::Lt => SelectOp::Gt,
            SelectOp::Gte => SelectOp::Lte,
            SelectOp::Lte => SelectOp::Gte,
        }
    }
}

#[inline(always)]
fn equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left == right
}

#[inline(always)]
fn not_equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left != right
}

#[inline(always)]
fn greater_than<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left > right
}

#[inline(always)]
fn greater_than_equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left >= right
}

#[inline(always)]
fn less_than<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left < right
}

#[inline(always)]
fn less_than_equal<T>(left: T, right: T) -> bool
where T: std::cmp::PartialOrd {
    left <= right
}

pub fn selection_op<T>(op: SelectOp) -> fn(T, T) -> bool
where T: std::cmp::PartialOrd {
    match op {
        SelectOp::Equal => equal::<T>,
        SelectOp::NotEqual => not_equal::<T>,
        SelectOp::Gt => greater_than::<T>,
        SelectOp::Gte => greater_than_equal::<T>,
        SelectOp::Lt => less_than::<T>,
        SelectOp::Lte => less_than_equal::<T>,
    }
}

// Build a range selection from a selection array.
pub fn build_range_selection(selection: &[u32], count: usize) -> Vec<Range<u32>> {
    let mut range_selection = Vec::with_capacity(count);
    let mut start = selection[0];
    let mut idx = 1;
    while idx < count {
        if selection[idx] != selection[idx - 1] + 1 {
            range_selection.push(start..selection[idx - 1] + 1);
            start = selection[idx];
        }
        idx += 1;
    }
    range_selection.push(start..selection[count - 1] + 1);
    range_selection
}
