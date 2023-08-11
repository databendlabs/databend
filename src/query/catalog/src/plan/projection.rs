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
use parquet_rs::arrow::ProjectionMask;
use parquet_rs::schema::types::SchemaDescriptor;

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

    pub fn to_arrow_projection(&self, schema: &SchemaDescriptor) -> ProjectionMask {
        match self {
            Projection::Columns(indices) => ProjectionMask::roots(schema, indices.clone()),
            Projection::InnerColumns(path_indices) => {
                let mut leave_id = 0;
                let tree = build_parquet_schema_tree(schema.root_schema(), &mut leave_id);
                assert_eq!(leave_id, schema.num_columns());
                let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                let mut leaves = vec![];
                for path in paths {
                    traverse_parquet_schema_tree(&tree, path, &mut leaves);
                }
                ProjectionMask::leaves(schema, leaves)
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

#[derive(Debug, PartialEq, Eq)]
enum ParquetSchemaTreeNode {
    Leaf(usize),
    Inner(Vec<ParquetSchemaTreeNode>),
}

fn build_parquet_schema_tree(
    ty: &parquet_rs::schema::types::Type,
    leave_id: &mut usize,
) -> ParquetSchemaTreeNode {
    match ty {
        parquet_rs::schema::types::Type::PrimitiveType { .. } => {
            let res = ParquetSchemaTreeNode::Leaf(*leave_id);
            *leave_id += 1;
            res
        }
        parquet_rs::schema::types::Type::GroupType { fields, .. } => {
            let mut children = Vec::with_capacity(fields.len());
            for field in fields.iter() {
                children.push(build_parquet_schema_tree(field, leave_id));
            }
            ParquetSchemaTreeNode::Inner(children)
        }
    }
}

fn traverse_parquet_schema_tree(
    node: &ParquetSchemaTreeNode,
    path: &[FieldIndex],
    leaves: &mut Vec<usize>,
) {
    match node {
        ParquetSchemaTreeNode::Leaf(id) => {
            leaves.push(*id);
        }
        ParquetSchemaTreeNode::Inner(children) => {
            if path.is_empty() {
                // All children should be included.
                for child in children.iter() {
                    traverse_parquet_schema_tree(child, path, leaves);
                }
            } else {
                let child = path[0];
                traverse_parquet_schema_tree(&children[child], &path[1..], leaves);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_expression::types::NumberDataType;
    use common_expression::TableDataType;
    use common_expression::TableField;
    use common_expression::TableSchema;
    use parquet_rs::arrow::arrow_to_parquet_schema;

    use super::build_parquet_schema_tree;
    use crate::plan::projection::ParquetSchemaTreeNode;

    #[test]
    fn test_build_parquet_schema_tree() {
        // Test schema (6 physical columns):
        // a: Int32,            (leave id: 0, path: [0])
        // b: Tuple (
        //    c: Int32,         (leave id: 1, path: [1, 0])
        //    d: Tuple (
        //        e: Int32,     (leave id: 2, path: [1, 1, 0])
        //        f: String,    (leave id: 3, path: [1, 1, 1])
        //    ),
        //    g: String,        (leave id: 4, path: [1, 2])
        // )
        // h: String,           (leave id: 5, path: [2])
        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Tuple {
                fields_name: vec!["c".to_string(), "d".to_string(), "g".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Tuple {
                        fields_name: vec!["e".to_string(), "f".to_string()],
                        fields_type: vec![
                            TableDataType::Number(NumberDataType::Int32),
                            TableDataType::String,
                        ],
                    },
                    TableDataType::String,
                ],
            }),
            TableField::new("h", TableDataType::String),
        ]);
        let arrow_fields = schema.to_arrow().fields;
        let arrow_schema = arrow_schema::Schema::new(
            arrow_fields
                .into_iter()
                .map(arrow_schema::Field::from)
                .collect::<Vec<_>>(),
        );
        let schema_desc = arrow_to_parquet_schema(&arrow_schema).unwrap();
        let mut leave_id = 0;
        let tree = build_parquet_schema_tree(schema_desc.root_schema(), &mut leave_id);
        assert_eq!(leave_id, 6);
        let expected_tree = ParquetSchemaTreeNode::Inner(vec![
            ParquetSchemaTreeNode::Leaf(0),
            ParquetSchemaTreeNode::Inner(vec![
                ParquetSchemaTreeNode::Leaf(1),
                ParquetSchemaTreeNode::Inner(vec![
                    ParquetSchemaTreeNode::Leaf(2),
                    ParquetSchemaTreeNode::Leaf(3),
                ]),
                ParquetSchemaTreeNode::Leaf(4),
            ]),
            ParquetSchemaTreeNode::Leaf(5),
        ]);
        assert_eq!(tree, expected_tree);
    }
}
