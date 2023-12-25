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

//! This module provides data structures for build column indexes.
//! It's used by Fuse Engine and Parquet Engine.

use databend_common_arrow::arrow::datatypes::DataType as ArrowType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::parquet::read::InitNested;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchema;

#[derive(Debug, Clone)]
pub struct ColumnNodes {
    pub column_nodes: Vec<ColumnNode>,
}

impl ColumnNodes {
    pub fn new_from_schema(schema: &ArrowSchema, table_schema: Option<&TableSchema>) -> Self {
        let mut leaf_id = 0;
        let mut column_nodes = Vec::with_capacity(schema.fields.len());

        let leaf_column_ids = table_schema.map(|table_schema| table_schema.to_leaf_column_ids());
        for field in &schema.fields {
            let mut column_node = Self::traverse_fields_dfs(field, false, vec![], &mut leaf_id);
            if let Some(ref leaf_column_ids) = leaf_column_ids {
                column_node.build_leaf_column_ids(leaf_column_ids);
            }
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
        is_nested: bool,
        init: Vec<InitNested>,
        leaf_id: &mut usize,
    ) -> ColumnNode {
        match &field.data_type {
            ArrowType::Struct(inner_fields) => {
                let mut child_column_nodes = Vec::with_capacity(inner_fields.len());
                let mut child_leaf_ids = Vec::with_capacity(inner_fields.len());
                for inner_field in inner_fields {
                    let mut inner_init = init.clone();
                    inner_init.push(InitNested::Struct(field.is_nullable));
                    let child_column_node =
                        Self::traverse_fields_dfs(inner_field, true, inner_init, leaf_id);
                    child_leaf_ids.extend(child_column_node.leaf_indices.clone());
                    child_column_nodes.push(child_column_node);
                }
                ColumnNode::new(
                    field.clone(),
                    true,
                    init,
                    child_leaf_ids,
                    Some(child_column_nodes),
                )
            }
            ArrowType::List(inner_field)
            | ArrowType::LargeList(inner_field)
            | ArrowType::FixedSizeList(inner_field, _) => {
                let mut inner_init = init.clone();
                inner_init.push(InitNested::List(field.is_nullable));
                let mut child_column_nodes = Vec::with_capacity(1);
                let mut child_leaf_ids = Vec::with_capacity(1);
                let child_column_node =
                    Self::traverse_fields_dfs(inner_field, true, inner_init, leaf_id);
                child_leaf_ids.extend(child_column_node.leaf_indices.clone());
                child_column_nodes.push(child_column_node);
                ColumnNode::new(
                    field.clone(),
                    true,
                    init,
                    child_leaf_ids,
                    Some(child_column_nodes),
                )
            }
            ArrowType::Map(inner_field, _) => {
                let mut inner_init = init.clone();
                inner_init.push(InitNested::List(field.is_nullable));
                let mut child_column_nodes = Vec::with_capacity(1);
                let mut child_leaf_ids = Vec::with_capacity(1);
                let child_column_node =
                    Self::traverse_fields_dfs(inner_field, true, inner_init, leaf_id);
                child_leaf_ids.extend(child_column_node.leaf_indices.clone());
                child_column_nodes.push(child_column_node);
                ColumnNode::new(
                    field.clone(),
                    true,
                    init,
                    child_leaf_ids,
                    Some(child_column_nodes),
                )
            }
            _ => {
                let column_node =
                    ColumnNode::new(field.clone(), is_nested, init, vec![*leaf_id], None);
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

    pub fn traverse_path_nested_aware<'a>(
        column_nodes: &'a [ColumnNode],
        path: &'a [usize],
        is_nested: bool,
    ) -> Result<(&'a ColumnNode, bool)> {
        let column_node = &column_nodes[path[0]];
        let is_nested = is_nested || column_node.children.is_some();
        if path.len() > 1 {
            return match &column_node.children {
                Some(ref children) => {
                    Self::traverse_path_nested_aware(children, &path[1..], is_nested)
                }
                None => Err(ErrorCode::Internal(format!(
                    "Cannot get column_node by path: {:?}",
                    path
                ))),
            };
        }
        Ok((column_node, is_nested))
    }
}

/// `ColumnNode` contains all the leaf column ids of the column.
/// For the nested types, it may contain more than one leaf column.
#[derive(Debug, Clone)]
pub struct ColumnNode {
    pub field: ArrowField,
    // Array/Struct column or inner column of nested data types.
    pub is_nested: bool,
    // The initial info of nested data types, used to read inner field of struct column.
    pub init: Vec<InitNested>,
    // `leaf_indices` is the indices of all the leaf columns in DFS order,
    // through which we can find the meta information of the leaf columns.
    pub leaf_indices: Vec<FieldIndex>,
    // Optional children column for nested types.
    pub children: Option<Vec<ColumnNode>>,
    pub leaf_column_ids: Vec<ColumnId>,
}

impl ColumnNode {
    pub fn new(
        field: ArrowField,
        is_nested: bool,
        init: Vec<InitNested>,
        leaf_indices: Vec<usize>,
        children: Option<Vec<ColumnNode>>,
    ) -> Self {
        Self {
            field,
            is_nested,
            init,
            leaf_indices,
            children,
            leaf_column_ids: vec![],
        }
    }

    pub fn has_children(&self) -> bool {
        self.children.is_some()
    }

    pub fn build_leaf_column_ids(&mut self, leaf_column_ids: &Vec<u32>) {
        let mut node_leaf_column_ids = Vec::with_capacity(self.leaf_indices.len());
        for index in &self.leaf_indices {
            node_leaf_column_ids.push(leaf_column_ids[*index]);
        }
        self.leaf_column_ids = node_leaf_column_ids;

        if let Some(ref children) = self.children {
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                let mut new_child = child.clone();
                new_child.build_leaf_column_ids(leaf_column_ids);
                new_children.push(new_child);
            }

            self.children = Some(new_children);
        }
    }
}
