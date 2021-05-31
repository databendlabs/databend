// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::UInt64Builder;
use common_datavalues::*;

use crate::*;

#[test]
fn test_data_block_scatter() -> anyhow::Result<()> {
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
