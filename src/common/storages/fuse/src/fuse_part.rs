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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::Compression;
use common_planners::PartInfo;
use common_planners::PartInfoPtr;
use common_planners::Projection;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub num_values: u64,
}

impl ColumnMeta {
    pub fn create(offset: u64, length: u64, num_values: u64) -> ColumnMeta {
        ColumnMeta {
            offset,
            length,
            num_values,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FusePartInfo {
    pub location: String,
    /// FusePartInfo itself is not versioned
    /// the `format_version` is the version of the block which the `location` points to
    pub format_version: u64,
    pub nums_rows: usize,
    pub columns_meta: HashMap<usize, ColumnMeta>,
    pub compression: Compression,
}

#[typetag::serde(name = "fuse")]
impl PartInfo for FusePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl FusePartInfo {
    pub fn create(
        location: String,
        format_version: u64,
        rows_count: u64,
        columns_meta: HashMap<usize, ColumnMeta>,
        compression: Compression,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FusePartInfo {
            location,
            format_version,
            columns_meta,
            nums_rows: rows_count as usize,
            compression,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FusePartInfo> {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::LogicalError(
                "Cannot downcast from PartInfo to FusePartInfo.",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnLeaves {
    pub column_leaves: Vec<ColumnLeaf>,
}

impl ColumnLeaves {
    pub fn new_from_schema(schema: &ArrowSchema) -> Self {
        let mut leaf_id = 0;
        let mut column_leaves = Vec::with_capacity(schema.fields.len());

        for field in &schema.fields {
            let column_leaf = Self::traverse_fields_dfs(field, &mut leaf_id);
            column_leaves.push(column_leaf);
        }

        Self { column_leaves }
    }

    fn traverse_fields_dfs(field: &ArrowField, leaf_id: &mut usize) -> ColumnLeaf {
        match &field.data_type {
            ArrowType::Struct(inner_fields) => {
                let mut child_column_leaves = Vec::with_capacity(inner_fields.len());
                let mut child_leaf_ids = Vec::with_capacity(inner_fields.len());
                for inner_field in inner_fields {
                    let child_column_leaf = Self::traverse_fields_dfs(inner_field, leaf_id);
                    child_leaf_ids.extend(child_column_leaf.leaf_ids.clone());
                    child_column_leaves.push(child_column_leaf);
                }
                ColumnLeaf::new(field.clone(), child_leaf_ids, Some(child_column_leaves))
            }
            _ => {
                let column_leaf = ColumnLeaf::new(field.clone(), vec![*leaf_id], None);
                *leaf_id += 1;
                column_leaf
            }
        }
    }

    pub fn get_by_projection<'a>(&'a self, proj: &'a Projection) -> Result<Vec<&ColumnLeaf>> {
        let column_leaves = match proj {
            Projection::Columns(indices) => indices
                .iter()
                .map(|idx| &self.column_leaves[*idx])
                .collect(),
            Projection::InnerColumns(path_indices) => {
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                paths
                    .iter()
                    .map(|path| Self::traverse_path(&self.column_leaves, path).unwrap())
                    .collect()
            }
        };
        Ok(column_leaves)
    }

    fn traverse_path<'a>(
        column_leaves: &'a [ColumnLeaf],
        path: &'a [usize],
    ) -> Result<&'a ColumnLeaf> {
        let column_leaf = &column_leaves[path[0]];
        if path.len() > 1 {
            return match &column_leaf.children {
                Some(ref children) => Self::traverse_path(children, &path[1..]),
                None => Err(ErrorCode::LogicalError(format!(
                    "Cannot get column_leaf by path: {:?}",
                    path
                ))),
            };
        }
        Ok(column_leaf)
    }
}

/// `ColumnLeaf` contains all the leaf column ids of the column.
/// For the nested types, it may contain more than one leaf column.
#[derive(Debug, Clone)]
pub struct ColumnLeaf {
    pub field: ArrowField,
    // `leaf_ids` is the indices of all the leaf columns in DFS order,
    // through which we can find the meta information of the leaf columns.
    pub leaf_ids: Vec<usize>,
    // Optional children column for nested types.
    pub children: Option<Vec<ColumnLeaf>>,
}

impl ColumnLeaf {
    pub fn new(field: ArrowField, leaf_ids: Vec<usize>, children: Option<Vec<ColumnLeaf>>) -> Self {
        Self {
            field,
            leaf_ids,
            children,
        }
    }
}
