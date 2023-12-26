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

    #[inline]
    pub fn expect(&self) -> fn(Ordering) -> bool {
        match self {
            SelectOp::Equal => Self::equal,
            SelectOp::NotEqual => Self::not_equal,
            SelectOp::Gt => Self::greater_than,
            SelectOp::Lt => Self::less_than,
            SelectOp::Gte => Self::greater_than_equal,
            SelectOp::Lte => Self::less_than_equal,
        }
    }

    #[inline(always)]
    fn equal(res: Ordering) -> bool {
        matches!(res, std::cmp::Ordering::Equal)
    }

    #[inline(always)]
    fn not_equal(res: Ordering) -> bool {
        !matches!(res, std::cmp::Ordering::Equal)
    }

    #[inline(always)]
    fn greater_than(res: Ordering) -> bool {
        matches!(res, std::cmp::Ordering::Greater)
    }

    #[inline(always)]
    fn less_than(res: Ordering) -> bool {
        matches!(res, std::cmp::Ordering::Less)
    }

    #[inline(always)]
    fn greater_than_equal(res: Ordering) -> bool {
        !matches!(res, std::cmp::Ordering::Less)
    }

    #[inline(always)]
    fn less_than_equal(res: Ordering) -> bool {
        !matches!(res, std::cmp::Ordering::Greater)
    }
}
