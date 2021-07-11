// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;

use crate::*;

#[test]
fn test_data_block_group_by() -> anyhow::Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::Utf8, false),
    ]);

    let block = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![1i8, 1, 2, 1, 2, 3]),
        Series::new(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let columns = &["a".to_string(), "b".to_string()];
    let method = HashMethodSerializer::default();
    let table = method.group_by(&block, columns)?;
    for (_, _, block) in table {
        match block.num_rows() {
            1 => {
                let expected = vec![
                    "+---+----+",
                    "| a | b  |",
                    "+---+----+",
                    "| 3 | x3 |",
                    "+---+----+",
                ];
                crate::assert_blocks_sorted_eq(expected, &[block]);
            }
            2 => {
                let expected = vec![
                    "+---+----+",
                    "| a | b  |",
                    "+---+----+",
                    "| 2 | x2 |",
                    "| 2 | x2 |",
                    "+---+----+",
                ];
                crate::assert_blocks_sorted_eq(expected, &[block]);
            }
            3 => {
                let expected = vec![
                    "+---+----+",
                    "| a | b  |",
                    "+---+----+",
                    "| 1 | x1 |",
                    "| 1 | x1 |",
                    "| 1 | x1 |",
                    "+---+----+",
                ];
                crate::assert_blocks_sorted_eq(expected, &[block]);
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
