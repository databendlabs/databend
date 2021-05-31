// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::*;

use crate::*;

#[test]
fn test_data_block_concat() -> anyhow::Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Utf8, false),
    ]);

    let blocks = vec![
        DataBlock::create_by_array(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["b1", "b2", "b3"])),
        ]),
        DataBlock::create_by_array(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])),
            Arc::new(StringArray::from(vec!["b1", "b2", "b3"])),
        ]),
        DataBlock::create_by_array(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![7, 8, 9])),
            Arc::new(StringArray::from(vec!["b1", "b2", "b3"])),
        ]),
    ];

    let results = DataBlock::concat_blocks(&blocks)?;
    assert_eq!(blocks[0].schema(), results.schema());

    let expected = vec![
        "+---+----+",
        "| a | b  |",
        "+---+----+",
        "| 1 | b1 |",
        "| 2 | b2 |",
        "| 3 | b3 |",
        "| 4 | b1 |",
        "| 5 | b2 |",
        "| 6 | b3 |",
        "| 7 | b1 |",
        "| 8 | b2 |",
        "| 9 | b3 |",
        "+---+----+",
    ];
    crate::assert_blocks_eq(expected, &[results]);
    Ok(())
}
