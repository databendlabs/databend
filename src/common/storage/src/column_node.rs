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

//! This module provides data structures for build column indexes.
//! It's used by Fuse Engine and Parquet Engine.

use std::collections::BTreeMap;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_exception::ErrorCode;
use common_exception::Result;

#[derive(Debug, Clone)]
pub struct ColumnNodes {
    pub column_nodes: Vec<ColumnNode>,
}

impl ColumnNodes {
    pub fn new_from_schema(
        schema: &ArrowSchema,
        column_id_map: Option<&BTreeMap<String, u32>>,
    ) -> Self {
        let mut leaf_id = 0;
        let mut column_nodes = Vec::with_capacity(schema.fields.len());

        for field in &schema.fields {
            let column_node =
                Self::traverse_fields_dfs(field, &field.name, column_id_map, &mut leaf_id);
            column_nodes.push(column_node);
        }

        Self { column_nodes }
    }

    /// Traverse the fields in DFS order to get [`ColumnNode`].
    ///
    /// If the data type is [`ArrowType::Struct`], we should expand its inner fields.
    ///
    /// If the data type is [`ArrowType::List`] or other nested types, we should also expand its inner field.
    /// It's because the inner field can also be [`ArrowType::Struct`] or other nested types.
    /// If we don't dfs into it, the inner columns information will be lost.
    /// and we can not construct the arrow-parquet reader correctly.
    fn traverse_fields_dfs(
        field: &ArrowField,
        parent_name: &String,
        column_id_map: Option<&BTreeMap<String, u32>>,
        leaf_id: &mut usize,
    ) -> ColumnNode {
        match &field.data_type {
            ArrowType::Struct(inner_fields) => {
                let mut child_column_nodes = Vec::with_capacity(inner_fields.len());
                let mut child_leaf_ids = Vec::with_capacity(inner_fields.len());
                let mut child_leaf_column_ids = Vec::with_capacity(inner_fields.len());
                for inner_field in inner_fields {
                    let inner_field_name = format!("{}:{}", parent_name, inner_field.name);
                    let child_column_node = Self::traverse_fields_dfs(
                        inner_field,
                        &inner_field_name,
                        column_id_map,
                        leaf_id,
                    );
                    child_leaf_ids.extend(child_column_node.leaf_ids.clone());
                    child_leaf_column_ids.extend(child_column_node.leaf_column_ids.clone());
                    child_column_nodes.push(child_column_node);
                }
                ColumnNode::new(
                    field.clone(),
                    child_leaf_ids,
                    Some(child_column_nodes),
                    child_leaf_column_ids,
                )
            }
            ArrowType::List(inner_field)
            | ArrowType::LargeList(inner_field)
            | ArrowType::FixedSizeList(inner_field, _) => {
                let mut child_column_nodes = Vec::with_capacity(1);
                let mut child_leaf_ids = Vec::with_capacity(1);
                let inner_field_name = &format!("{}:0", parent_name);
                let child_column_node = Self::traverse_fields_dfs(
                    inner_field,
                    inner_field_name,
                    column_id_map,
                    leaf_id,
                );
                child_leaf_ids.extend(child_column_node.leaf_ids.clone());
                let child_leaf_column_ids = child_column_node.leaf_column_ids.clone();
                child_column_nodes.push(child_column_node);
                ColumnNode::new(
                    field.clone(),
                    child_leaf_ids,
                    Some(child_column_nodes),
                    child_leaf_column_ids,
                )
            }
            _ => {
                let leaf_column_ids = match column_id_map {
                    Some(column_id_map) => {
                        vec![*column_id_map.get(parent_name).unwrap()]
                    }
                    None => vec![],
                };
                let column_node =
                    ColumnNode::new(field.clone(), vec![*leaf_id], None, leaf_column_ids);
                *leaf_id += 1;
                column_node
            }
        }
    }

    pub fn traverse_path<'a>(
        column_nodes: &'a [ColumnNode],
        path: &'a [usize],
    ) -> Result<&'a ColumnNode> {
        let column_node = &column_nodes[path[0]];
        if path.len() > 1 {
            return match &column_node.children {
                Some(ref children) => Self::traverse_path(children, &path[1..]),
                None => Err(ErrorCode::Internal(format!(
                    "Cannot get column_node by path: {:?}",
                    path
                ))),
            };
        }
        Ok(column_node)
    }

    fn build_column_id_map_from_leaf(
        parent_name: &String,
        node: &ColumnNode,
        column_ids: &mut BTreeMap<String, u32>,
    ) {
        match node.children {
            Some(ref children) => {
                for child in children {
                    let inner_field_name = format!("{}:{}", parent_name, child.field.name);
                    Self::build_column_id_map_from_leaf(&inner_field_name, child, column_ids);
                }
            }
            None => {
                column_ids.insert(parent_name.to_string(), node.leaf_ids[0] as u32);
            }
        }
    }

    pub fn build_column_id_map(&self) -> BTreeMap<String, u32> {
        let mut column_ids = BTreeMap::new();
        for column_node in &self.column_nodes {
            Self::build_column_id_map_from_leaf(
                &column_node.field.name,
                column_node,
                &mut column_ids,
            );
        }

        column_ids
    }
}

/// `ColumnNode` contains all the leaf column ids of the column.
/// For the nested types, it may contain more than one leaf column.
#[derive(Debug, Clone)]
pub struct ColumnNode {
    pub field: ArrowField,
    // `leaf_ids` is the indices of all the leaf columns in DFS order,
    // through which we can find the meta information of the leaf columns.
    pub leaf_ids: Vec<usize>,
    // Optional children column for nested types.
    pub children: Option<Vec<ColumnNode>>,
    pub leaf_column_ids: Vec<u32>,
}

impl ColumnNode {
    pub fn new(
        field: ArrowField,
        leaf_ids: Vec<usize>,
        children: Option<Vec<ColumnNode>>,
        leaf_column_ids: Vec<u32>,
    ) -> Self {
        Self {
            field,
            leaf_ids,
            children,
            leaf_column_ids,
        }
    }

    // Handle the case that leaf_column_ids is empty
    pub fn leaf_column_id(&self, i: usize) -> u32 {
        if self.leaf_ids.len() == self.leaf_column_ids.len() {
            self.leaf_column_ids[i]
        } else {
            self.leaf_ids[i] as u32
        }
    }
}
