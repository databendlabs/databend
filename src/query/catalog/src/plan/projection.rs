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

use std::collections::BTreeMap;
use std::fmt::Formatter;

use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::TableSchema;
use common_storage::ColumnNode;
use common_storage::ColumnNodes;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub enum Projection {
    /// column indices of the table
    Columns(Vec<FieldIndex>),
    /// inner column indices for tuple data type with inner columns.
    /// the key is the column_index of ColumnEntry.
    /// the value is the path indices of inner columns.
    InnerColumns(BTreeMap<FieldIndex, Vec<FieldIndex>>),
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
    pub fn project_schema(&self, schema: &TableSchema) -> TableSchema {
        match self {
            Projection::Columns(indices) => schema.project(indices),
            Projection::InnerColumns(path_indices) => schema.inner_project(path_indices),
        }
    }

    pub fn project_column_nodes<'a>(
        &'a self,
        column_nodes: &'a ColumnNodes,
    ) -> Result<Vec<&ColumnNode>> {
        let column_nodes = match self {
            Projection::Columns(indices) => indices
                .iter()
                .map(|idx| &column_nodes.column_nodes[*idx])
                .collect(),
            Projection::InnerColumns(path_indices) => {
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                paths
                    .iter()
                    .map(|path| ColumnNodes::traverse_path(&column_nodes.column_nodes, path))
                    .collect::<Result<_>>()?
            }
        };
        Ok(column_nodes)
    }

    pub fn add_col(&mut self, col: FieldIndex) {
        match self {
            Projection::Columns(indices) => {
                if indices.contains(&col) {
                    return;
                }
                indices.push(col);
                indices.sort();
            }
            Projection::InnerColumns(path_indices) => {
                path_indices.entry(col).or_insert(vec![col]);
            }
        }
    }

    pub fn remove_col(&mut self, col: FieldIndex) {
        match self {
            Projection::Columns(indices) => {
                if let Some(pos) = indices.iter().position(|x| *x == col) {
                    indices.remove(pos);
                }
            }
            Projection::InnerColumns(path_indices) => {
                path_indices.remove(&col);
            }
        }
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
