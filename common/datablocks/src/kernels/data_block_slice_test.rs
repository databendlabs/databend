// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::*;

#[test]
fn test_data_block_slice() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Float64, false),
    ]);

    let raw = DataBlock::create(schema.clone(), vec![
        Series::new(vec![1i64, 2, 3, 4, 5]).into(),
        Series::new(vec![1.0f64, 2., 3., 4., 5.]).into(),
    ]);

    let sliced = DataBlock::split_block_by_size(&raw, 1)?;
    assert_eq!(sliced.len(), 5);

    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 1 |",
        "| 2 | 2 |",
        "| 3 | 3 |",
        "| 4 | 4 |",
        "| 5 | 5 |",
        "+---+---+",
    ];
    crate::assert_blocks_eq(expected, &sliced);
    Ok(())
}
