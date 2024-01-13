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

use databend_common_exception::Result;
use databend_common_expression::create_test_complex_schema;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_storage::ColumnNode;
use databend_common_storage::ColumnNodes;

fn test_node_leaf_index_match_column_ids(schema: &TableSchema, column_node: &ColumnNode) {
    let leaf_column_ids = schema.to_leaf_column_ids();

    for (j, lead_index) in column_node.leaf_indices.iter().enumerate() {
        assert_eq!(leaf_column_ids[*lead_index], column_node.leaf_column_ids[j]);
    }
}

fn test_column_nodes_index_match_column_ids(schema: &TableSchema, column_leaves: &ColumnNodes) {
    for column_node in &column_leaves.column_nodes {
        test_node_leaf_index_match_column_ids(schema, column_node);
        if let Some(ref children) = column_node.children {
            for child in children {
                test_node_leaf_index_match_column_ids(schema, child);
            }
        }
    }
}

#[test]
fn test_column_leaf_schema_from_struct() -> Result<()> {
    let schema = create_test_complex_schema();

    let column_leaves = ColumnNodes::new_from_schema(&(&schema).into(), Some(&schema));
    let column_1_ids = vec![0];
    let column_2_ids = vec![1, 2, 3];
    let column_3_ids = vec![4, 5];
    let column_4_ids = vec![6];
    let column_5_ids = vec![7, 8];
    let column_6_ids = vec![9];
    let column_7_ids = vec![10];
    let column_8_ids = vec![11, 12];
    let expected_column_ids = [
        ("u64", &column_1_ids),
        ("tuplearray", &column_2_ids),
        ("arraytuple", &column_3_ids),
        ("nullarray", &column_4_ids),
        ("maparray", &column_5_ids),
        ("nullu64", &column_6_ids),
        ("u64array", &column_7_ids),
        ("tuplesimple", &column_8_ids),
    ];

    for (i, column_leaf) in column_leaves.column_nodes.iter().enumerate() {
        let expected_column_id = expected_column_ids[i];
        assert_eq!(expected_column_id.0.to_string(), column_leaf.field.name);
        assert_eq!(*expected_column_id.1, column_leaf.leaf_column_ids);
    }

    test_column_nodes_index_match_column_ids(&schema, &column_leaves);

    Ok(())
}

#[test]
fn test_column_leaf_schema_from_struct_of_old_version() -> Result<()> {
    let old_schema = create_test_complex_schema();
    let old_column_leaves = ColumnNodes::new_from_schema(&(&old_schema).into(), None);

    let new_schema = TableSchema::init_if_need(old_schema);
    let new_column_leaves = ColumnNodes::new_from_schema(&(&new_schema).into(), Some(&new_schema));

    for (old_leaf, new_leaf) in old_column_leaves
        .column_nodes
        .iter()
        .zip(new_column_leaves.column_nodes.iter())
    {
        assert_eq!(old_leaf.field.name, new_leaf.field.name);
        assert_eq!(old_leaf.leaf_indices, new_leaf.leaf_indices);

        // assert new column node column ids equal to old column node leaf ids.
        for (leaf_id, column_id) in old_leaf
            .leaf_indices
            .iter()
            .zip(new_leaf.leaf_column_ids.iter())
        {
            assert_eq!(*leaf_id as u32, *column_id);
        }
    }

    Ok(())
}

// add/drop column from schema, and test column_ids match leaf_indices
#[test]
fn test_alter_schema_column() -> Result<()> {
    let fields = vec![TableField::new(
        "exist_column",
        TableDataType::Number(NumberDataType::UInt64),
    )];
    let mut schema = TableSchema::new(fields);

    let old_schema = create_test_complex_schema();

    // test add column
    for field in old_schema.fields() {
        schema.add_columns(&[TableField::new(field.name(), field.data_type().to_owned())])?;

        let column_leaves = ColumnNodes::new_from_schema(&(&schema).into(), Some(&schema));
        test_column_nodes_index_match_column_ids(&schema, &column_leaves);
    }

    // test drop column
    for field in old_schema.fields() {
        schema.drop_column(field.name())?;

        let column_leaves = ColumnNodes::new_from_schema(&(&schema).into(), Some(&schema));
        test_column_nodes_index_match_column_ids(&schema, &column_leaves);
    }

    Ok(())
}
