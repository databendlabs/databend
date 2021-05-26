// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::UInt64Builder;
use common_exception::ErrorCodes;

#[test]
fn test_data_block_kernel_take() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;

    use crate::*;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Utf8, false),
    ]);

    let raw = DataBlock::create_by_array(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["b1", "b2", "b3"])),
    ]);

    let take = DataBlock::block_take_by_indices(&raw, &[0, 2])?;
    assert_eq!(raw.schema(), take.schema());

    let expected = vec![
        "+---+----+",
        "| a | b  |",
        "+---+----+",
        "| 1 | b1 |",
        "| 3 | b3 |",
        "+---+----+",
    ];
    crate::assert_blocks_eq(expected, &[take]);

    Ok(())
}

#[test]
fn test_data_block_kernel_concat() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;

    use crate::*;

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

#[test]
fn test_data_block_sort() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;

    use crate::data_block_kernel::SortColumnDescription;
    use crate::*;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Utf8, false),
    ]);

    let raw = DataBlock::create_by_array(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![6, 4, 3, 2, 1, 7])),
        Arc::new(StringArray::from(vec!["b1", "b2", "b3", "b4", "b5", "b6"])),
    ]);

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: true,
            nulls_first: false
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 1 | b5 |",
            "| 2 | b4 |",
            "| 3 | b3 |",
            "+---+----+",
        ];
        crate::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: false,
            nulls_first: false
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 7 | b6 |",
            "| 6 | b1 |",
            "| 4 | b2 |",
            "+---+----+",
        ];
        crate::assert_blocks_eq(expected, &[results]);
    }
    Ok(())
}

#[test]
fn test_scatter_block() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;

    use crate::*;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Float64, false),
    ]);

    let raw = DataBlock::create(schema.clone(), vec![
        DataColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2, 3]))),
        DataColumnarValue::Array(Arc::new(Float64Array::from(vec![1., 2., 3.]))),
    ]);

    let mut builder = UInt64Builder::new(3);
    builder.append_slice(&[0, 1, 0])?;
    let indices: DataArrayRef = Arc::new(builder.finish());

    let scattered = DataBlock::scatter_block(&raw, &indices, 2)?;
    assert_eq!(scattered.len(), 2);
    assert_eq!(raw.schema(), scattered[0].schema());
    assert_eq!(raw.schema(), scattered[1].schema());
    assert_eq!(scattered[0].num_rows(), 2);
    assert_eq!(scattered[1].num_rows(), 1);

    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 1 |",
        "| 3 | 3 |",
        "| 2 | 2 |",
        "+---+---+",
    ];
    crate::assert_blocks_eq(expected, &scattered);

    Ok(())
}
