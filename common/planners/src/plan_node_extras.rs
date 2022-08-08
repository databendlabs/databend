// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;

use crate::Expression;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Projection {
    /// column indices of the table
    Columns(Vec<usize>),
    /// inner column indices for tuple data type with inner columns.
    /// the key is the column_index of ColumnEntry.
    /// the value is the path indices of inner columns.
    InnerColumns(BTreeMap<usize, Vec<usize>>),
}

impl Projection {
    pub fn len(&self) -> usize {
        match self {
            Projection::Columns(indices) => indices.len(),
            Projection::InnerColumns(path_indices) => path_indices.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Projection::Columns(indices) => indices.is_empty(),
            Projection::InnerColumns(path_indices) => path_indices.is_empty(),
        }
    }
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Extras {
    /// Optional column indices to use as a projection
    pub projection: Option<Projection>,
    /// Optional filter expression plan
    pub filters: Vec<Expression>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
    /// Optional order_by expression plan
    pub order_by: Vec<Expression>,
}

impl Extras {
    pub fn default() -> Self {
        Extras {
            projection: None,
            filters: vec![],
            limit: None,
            order_by: vec![],
        }
    }
}
