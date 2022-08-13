// Copyright 2022 Datafuse Labs.
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

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Code in this file is mainly copied from apache/arrow-datafusion
// Original project code: https://github.com/apache/arrow-datafusion/blob/a6e93a10ab2659500eb4f838b7b53f138e545be3/datafusion/expr/src/window_frame.rs#L39
// PR: https://github.com/datafuselabs/databend/pull/5401

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use common_exception::ErrorCode;
use sqlparser::ast;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}

impl TryFrom<ast::WindowFrame> for WindowFrame {
    type Error = ErrorCode;

    fn try_from(value: ast::WindowFrame) -> Result<Self, Self::Error> {
        let start_bound = value.start_bound.into();
        let end_bound = value
            .end_bound
            .map(WindowFrameBound::from)
            .unwrap_or(WindowFrameBound::CurrentRow);

        if let WindowFrameBound::Following(None) = start_bound {
            Err(ErrorCode::LogicalError(
                "Invalid window frame: start bound cannot be unbounded following".to_owned(),
            ))
        } else if let WindowFrameBound::Preceding(None) = end_bound {
            Err(ErrorCode::LogicalError(
                "Invalid window frame: end bound cannot be unbounded preceding".to_owned(),
            ))
        } else if start_bound > end_bound {
            Err(ErrorCode::LogicalError(format!(
                "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
                start_bound, end_bound
            )))
        } else {
            let units = value.units.into();
            Ok(Self {
                units,
                start_bound,
                end_bound,
            })
        }
    }
}

impl Default for WindowFrame {
    fn default() -> Self {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: WindowFrameBound::CurrentRow,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum WindowFrameUnits {
    Range,
    Rows,
}

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Rows => "ROWS",
        })
    }
}

impl From<ast::WindowFrameUnits> for WindowFrameUnits {
    fn from(value: ast::WindowFrameUnits) -> Self {
        match value {
            ast::WindowFrameUnits::Range => Self::Range,
            ast::WindowFrameUnits::Rows => Self::Rows,
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameBound {
    Preceding(Option<u64>),
    CurrentRow,
    Following(Option<u64>),
}

impl WindowFrameBound {
    fn get_rank(&self) -> (u8, u64) {
        match self {
            WindowFrameBound::Preceding(None) => (0, 0),
            WindowFrameBound::Following(None) => (4, 0),
            WindowFrameBound::Preceding(Some(0))
            | WindowFrameBound::CurrentRow
            | WindowFrameBound::Following(Some(0)) => (2, 0),
            WindowFrameBound::Preceding(Some(v)) => (1, u64::MAX - *v),
            WindowFrameBound::Following(Some(v)) => (3, *v),
        }
    }
}

impl From<ast::WindowFrameBound> for WindowFrameBound {
    fn from(value: ast::WindowFrameBound) -> Self {
        match value {
            ast::WindowFrameBound::Preceding(v) => Self::Preceding(v),
            ast::WindowFrameBound::Following(v) => Self::Following(v),
            ast::WindowFrameBound::CurrentRow => Self::CurrentRow,
        }
    }
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }
    }
}

impl PartialEq for WindowFrameBound {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for WindowFrameBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WindowFrameBound {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_rank().cmp(&other.get_rank())
    }
}

impl Hash for WindowFrameBound {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_rank().hash(state)
    }
}
