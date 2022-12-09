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
use std::fmt::Formatter;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_storage::ColumnLeaf;
use common_storage::ColumnLeaves;

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

    pub fn project_column_leaves<'a>(
        &'a self,
        column_leaves: &'a ColumnLeaves,
    ) -> Result<Vec<&ColumnLeaf>> {
        let column_leaves = match self {
            Projection::Columns(indices) => indices
                .iter()
                .map(|idx| &column_leaves.column_leaves[*idx])
                .collect(),
            Projection::InnerColumns(path_indices) => {
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                paths
                    .iter()
                    .map(|path| {
                        ColumnLeaves::traverse_path(&column_leaves.column_leaves, path).unwrap()
                    })
                    .collect()
            }
        };
        Ok(column_leaves)
    }
}

impl core::fmt::Debug for Projection {
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
