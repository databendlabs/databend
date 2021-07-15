// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;

use crate::*;

#[test]
fn test_data_block_group_by_hash() -> anyhow::Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::Int8, false),
        DataField::new("c", DataType::Int8, false),
        DataField::new("x", DataType::Utf8, false),
    ]);

    let block = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![1i8, 1, 2, 1, 2, 3]),
        Series::new(vec![1i8, 1, 2, 1, 2, 3]),
        Series::new(vec![1i8, 1, 2, 1, 2, 3]),
        Series::new(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let method = DataBlock::choose_hash_method(&block, &vec!["a".to_string(), "x".to_string()])?;
    assert_eq!(
        method,
        HashMethodKind::Serializer(HashMethodSerializer::default())
    );

    let method = DataBlock::choose_hash_method(&block, &vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
    ])?;
    assert_eq!(
        method,
        HashMethodKind::KeysU32(HashMethodKeysU32::default())
    );

    let hash = HashMethodKeysU32::default();
    let columns = vec!["a", "b", "c"];

    let mut group_columns = Vec::with_capacity(columns.len());
    {
        for col in columns {
            group_columns.push(block.try_column_by_name(col)?);
        }
    }

    let keys = hash.build_keys(&group_columns, block.num_rows())?;
    assert_eq!(keys, vec![
        0x10101, 0x10101, 0x20202, 0x10101, 0x20202, 0x30303
    ]);
    Ok(())
}
