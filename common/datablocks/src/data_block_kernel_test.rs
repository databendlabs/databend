// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_data_block_kernel_take() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;

    use crate::*;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Utf8, false),
    ]));

    let raw = DataBlock::create(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["b1", "b2", "b3"])),
        ],
    );

    let take = block_take_by_indices(&raw, &[0, 2])?;
    assert_eq!(raw.schema(), take.schema());

    let expected = vec![
        "+---+----+",
        "| a | b  |",
        "+---+----+",
        "| 1 | b1 |",
        "| 3 | b3 |",
        "+---+----+",
    ];
    crate::assert_blocks_sorted_eq!(expected, vec![take]);

    Ok(())
}
