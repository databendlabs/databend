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
    pub fn not(&self) -> Self {
        match &self {
            SelectOp::Equal => SelectOp::NotEqual,
            SelectOp::NotEqual => SelectOp::Equal,
            SelectOp::Gt => SelectOp::Lte,
            SelectOp::Lt => SelectOp::Gte,
            SelectOp::Gte => SelectOp::Lt,
            SelectOp::Lte => SelectOp::Gt,
        }
    }
}

#[macro_export]
macro_rules! with_mapped_cmp_method {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [
                Equal => equal,
                NotEqual => not_equal,
                Gt => greater_than,
                Lt => less_than,
                Gte => greater_than_equal,
                Lte => less_than_equal,
            ],
            $($tail)*
        }
    }
}
