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

use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_storage::ColumnLeaves;

#[test]
fn test_column_leaf_schema_from_struct() -> Result<()> {
    let child_field11 = u64::to_data_type();
    let child_field12 = u64::to_data_type();
    let child_field22 = u64::to_data_type();

    let s = StructType::create(None, vec![child_field11, child_field12]);

    let s2 = StructType::create(None, vec![DataTypeImpl::Struct(s), child_field22]);

    let field1 = DataField::new("a", u64::to_data_type());
    let field2 = DataField::new("b", DataTypeImpl::Struct(s2));
    let field3 = DataField::new("c", u64::to_data_type());

    let schema = DataSchema::new(vec![field1, field2, field3]);

    let column_leaves =
        ColumnLeaves::new_from_schema(&schema.to_arrow(), Some(schema.column_id_map()));
    let column_1_ids = vec![0];
    let column_2_ids = vec![3, 4, 5];
    let column_3_ids = vec![6];
    let expeted_column_ids = vec![
        ("a", &column_1_ids),
        ("b", &column_2_ids),
        ("c", &column_3_ids),
    ];

    for (i, column_leaf) in column_leaves.column_leaves.iter().enumerate() {
        let expeted_column_id = expeted_column_ids[i];
        assert_eq!(expeted_column_id.0.to_string(), column_leaf.field.name);
        for (j, column_id) in column_leaf.leaf_column_ids.iter().enumerate() {
            assert_eq!(expeted_column_id.1[j], *column_id);
        }
    }

    Ok(())
}

#[test]
fn test_column_leaf_schema_from_struct_of_old_version() -> Result<()> {
    let child_field11 = u64::to_data_type();
    let child_field12 = u64::to_data_type();
    let child_field22 = u64::to_data_type();

    let s = StructType::create(None, vec![child_field11, child_field12]);

    let s2 = StructType::create(None, vec![DataTypeImpl::Struct(s), child_field22]);

    let field1 = DataField::new("a", u64::to_data_type());
    let field2 = DataField::new("b", DataTypeImpl::Struct(s2));
    let field3 = DataField::new("c", u64::to_data_type());

    let old_schema = DataSchema::new(vec![field1, field2, field3]);
    let old_column_leaves = ColumnLeaves::new_from_schema(&old_schema.to_arrow(), None);

    let new_schema = DataSchema::init_if_need(old_schema);
    let new_column_leaves =
        ColumnLeaves::new_from_schema(&new_schema.to_arrow(), Some(new_schema.column_id_map()));

    // make sure old and new schema build the same column id map
    assert_eq!(
        old_column_leaves.build_column_id_map(),
        new_column_leaves.build_column_id_map()
    );
    for (old_leaf, new_leaf) in old_column_leaves
        .column_leaves
        .iter()
        .zip(new_column_leaves.column_leaves.iter())
    {
        assert_eq!(old_leaf.field.name, new_leaf.field.name);
        assert_eq!(old_leaf.leaf_ids, new_leaf.leaf_ids);
        for (leaf_id, column_id) in old_leaf
            .leaf_ids
            .iter()
            .zip(new_leaf.leaf_column_ids.iter())
        {
            assert_eq!(*leaf_id as u32, *column_id);
        }
    }

    Ok(())
}
