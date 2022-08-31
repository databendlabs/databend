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
use std::fmt::Debug;
use std::fmt::Formatter;

use common_datavalues::DataSchema;

use crate::Expression;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
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

    /// Use this projection to project a schema.
    pub fn project_schema(&self, schema: &DataSchema) -> DataSchema {
        match self {
            Projection::Columns(indices) => schema.project(indices),
            Projection::InnerColumns(path_indices) => schema.inner_project(path_indices),
        }
    }
}

impl Debug for Projection {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Projection::Columns(indices) => write!(f, "{:?}", indices),
            Projection::InnerColumns(path_indices) => {
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                write!(f, "{:?}", paths)
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct PrewhereInfo {
    /// column indices of the table used for prewhere
    pub need_columns: Projection,
    /// remain_columns = scan.columns - need_columns
    pub remain_columns: Projection,
    /// filter for prewhere
    pub filter: Expression,
}

/// Extras is a wrapper for push down items.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Extras {
    /// Optional column indices to use as a projection
    pub projection: Option<Projection>,
    /// Optional filter expression plan
    /// split_conjunctions by `and` operator
    pub filters: Vec<Expression>,
    /// Optional prewhere information
    /// used for prewhere optimization
    pub prewhere: Option<PrewhereInfo>,
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
            prewhere: None,
            limit: None,
            order_by: vec![],
        }
    }
}
