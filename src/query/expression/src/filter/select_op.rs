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

use std::cmp::PartialOrd;

#[derive(Clone, PartialEq, Debug)]
pub enum SelectOp {
    // Equal "="
    Equal,
    // Not equal "!="
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

    #[inline]
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

/// Return the comparison function for the given select operation, some data types not support comparison.
#[inline(always)]
pub fn compare_operation<T: PartialOrd>(op: &SelectOp) -> fn(T, T) -> bool {
    match op {
        SelectOp::Equal => equal,
        SelectOp::NotEqual => not_equal,
        SelectOp::Gt => greater_than,
        SelectOp::Gte => greater_than_equal,
        SelectOp::Lt => less_than,
        SelectOp::Lte => less_than_equal,
    }
}

#[inline(always)]
fn equal<T: PartialOrd>(left: T, right: T) -> bool {
    left == right
}

#[inline(always)]
fn not_equal<T: PartialOrd>(left: T, right: T) -> bool {
    left != right
}

#[inline(always)]
fn greater_than<T: PartialOrd>(left: T, right: T) -> bool {
    left > right
}

#[inline(always)]
fn greater_than_equal<T: PartialOrd>(left: T, right: T) -> bool {
    left >= right
}

#[inline(always)]
fn less_than<T: PartialOrd>(left: T, right: T) -> bool {
    left < right
}

#[inline(always)]
fn less_than_equal<T: PartialOrd>(left: T, right: T) -> bool {
    left <= right
}
