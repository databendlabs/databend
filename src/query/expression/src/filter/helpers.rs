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

use core::cmp::Ordering;

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

#[inline(always)]
fn boolean_equal(left: bool, right: bool) -> bool {
    left == right
}

#[inline(always)]
fn boolean_not_equal(left: bool, right: bool) -> bool {
    left != right
}

#[inline(always)]
fn boolean_greater_than(left: bool, right: bool) -> bool {
    left & !right
}

#[inline(always)]
fn boolean_greater_than_equal(left: bool, right: bool) -> bool {
    (left & !right) || (left & right)
}

#[inline(always)]
fn boolean_less_than(left: bool, right: bool) -> bool {
    !left & right
}

#[inline(always)]
fn boolean_less_than_equal(left: bool, right: bool) -> bool {
    (!left & right) || (left & right)
}

pub fn boolean_selection_op(op: SelectOp) -> fn(bool, bool) -> bool {
    match op {
        SelectOp::Equal => boolean_equal,
        SelectOp::NotEqual => boolean_not_equal,
        SelectOp::Gt => boolean_greater_than,
        SelectOp::Gte => boolean_greater_than_equal,
        SelectOp::Lt => boolean_less_than,
        SelectOp::Lte => boolean_less_than_equal,
    }
}

#[inline(always)]
fn string_equal(left: &[u8], right: &[u8]) -> bool {
    left == right
}

#[inline(always)]
fn string_not_equal(left: &[u8], right: &[u8]) -> bool {
    left != right
}

#[inline(always)]
fn string_greater_than(left: &[u8], right: &[u8]) -> bool {
    left > right
}

#[inline(always)]
fn string_greater_than_equal(left: &[u8], right: &[u8]) -> bool {
    left >= right
}

#[inline(always)]
fn string_less_than(left: &[u8], right: &[u8]) -> bool {
    left < right
}

#[inline(always)]
fn string_less_than_equal(left: &[u8], right: &[u8]) -> bool {
    left <= right
}

pub fn string_selection_op(op: &SelectOp) -> fn(&[u8], &[u8]) -> bool {
    match op {
        SelectOp::Equal => string_equal,
        SelectOp::NotEqual => string_not_equal,
        SelectOp::Gt => string_greater_than,
        SelectOp::Gte => string_greater_than_equal,
        SelectOp::Lt => string_less_than,
        SelectOp::Lte => string_less_than_equal,
    }
}

#[inline(always)]
fn variant_equal(left: &[u8], right: &[u8]) -> bool {
    jsonb::compare(left, right).expect("unable to parse jsonb value") == Ordering::Equal
}

#[inline(always)]
fn variant_not_equal(left: &[u8], right: &[u8]) -> bool {
    jsonb::compare(left, right).expect("unable to parse jsonb value") != Ordering::Equal
}

#[inline(always)]
fn variant_greater_than(left: &[u8], right: &[u8]) -> bool {
    jsonb::compare(left, right).expect("unable to parse jsonb value") == Ordering::Greater
}

#[inline(always)]
fn variant_greater_than_equal(left: &[u8], right: &[u8]) -> bool {
    jsonb::compare(left, right).expect("unable to parse jsonb value") != Ordering::Less
}

#[inline(always)]
fn variant_less_than(left: &[u8], right: &[u8]) -> bool {
    jsonb::compare(left, right).expect("unable to parse jsonb value") == Ordering::Less
}

#[inline(always)]
fn variant_less_than_equal(left: &[u8], right: &[u8]) -> bool {
    jsonb::compare(left, right).expect("unable to parse jsonb value") != Ordering::Greater
}

pub fn variant_selection_op(op: &SelectOp) -> fn(&[u8], &[u8]) -> bool {
    match op {
        SelectOp::Equal => variant_equal,
        SelectOp::NotEqual => variant_not_equal,
        SelectOp::Gt => variant_greater_than,
        SelectOp::Gte => variant_greater_than_equal,
        SelectOp::Lt => variant_less_than,
        SelectOp::Lte => variant_less_than_equal,
    }
}

#[inline(always)]
fn tuple_equal<T>(left: T, right: T) -> Option<bool>
where T: std::cmp::PartialOrd {
    if left != right { Some(false) } else { None }
}

#[inline(always)]
fn tuple_not_equal<T>(left: T, right: T) -> Option<bool>
where T: std::cmp::PartialOrd {
    if left != right { Some(true) } else { None }
}

#[inline(always)]
fn tuple_greater_than<T>(left: T, right: T) -> Option<bool>
where T: std::cmp::PartialOrd {
    match left.partial_cmp(&right) {
        Some(Ordering::Greater) => Some(true),
        Some(Ordering::Less) => Some(false),
        _ => None,
    }
}

#[inline(always)]
fn tuple_greater_than_equal<T>(left: T, right: T) -> Option<bool>
where T: std::cmp::PartialOrd {
    match left.partial_cmp(&right) {
        Some(Ordering::Greater) => Some(true),
        Some(Ordering::Less) => Some(false),
        _ => None,
    }
}

#[inline(always)]
fn tuple_less_than<T>(left: T, right: T) -> Option<bool>
where T: std::cmp::PartialOrd {
    match left.partial_cmp(&right) {
        Some(Ordering::Less) => Some(true),
        Some(Ordering::Greater) => Some(false),
        _ => None,
    }
}

#[inline(always)]
fn tuple_less_than_equal<T>(left: T, right: T) -> Option<bool>
where T: std::cmp::PartialOrd {
    match left.partial_cmp(&right) {
        Some(Ordering::Less) => Some(true),
        Some(Ordering::Greater) => Some(false),
        _ => None,
    }
}

pub fn tuple_selection_op<T>(op: SelectOp) -> fn(T, T) -> Option<bool>
where T: std::cmp::PartialOrd {
    match op {
        SelectOp::Equal => tuple_equal::<T>,
        SelectOp::NotEqual => tuple_not_equal::<T>,
        SelectOp::Gt => tuple_greater_than::<T>,
        SelectOp::Gte => tuple_greater_than_equal::<T>,
        SelectOp::Lt => tuple_less_than::<T>,
        SelectOp::Lte => tuple_less_than_equal::<T>,
    }
}

pub fn tuple_compare_default_value(op: &SelectOp) -> bool {
    match op {
        SelectOp::Equal => true,
        SelectOp::NotEqual => false,
        SelectOp::Gt => false,
        SelectOp::Gte => true,
        SelectOp::Lt => false,
        SelectOp::Lte => true,
    }
}

pub fn empty_array_compare_value(op: &SelectOp) -> bool {
    match op {
        SelectOp::Equal => true,
        SelectOp::NotEqual => false,
        SelectOp::Gt => false,
        SelectOp::Gte => true,
        SelectOp::Lt => false,
        SelectOp::Lte => true,
    }
}
