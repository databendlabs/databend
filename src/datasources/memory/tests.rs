// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[async_std::test]
async fn test_memory_table() -> crate::error::Result<()> {
    use async_std::stream::StreamExt;
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datasources::*;
    use crate::datavalues::*;

    let mut mem = MemoryTable::create(
        "mem_table",
        Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Int64,
            false,
        )])),
    );

    let block = DataBlock::create(
        Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    );
    mem.add_partition("part_0", block)?;

    let mut stream = mem
        .read(vec![Partition {
            name: "part_0".to_string(),
            version: 0,
        }])
        .await?;

    let mut rows = 0;
    while let Some(v) = stream.next().await {
        let row = v?.num_rows();
        rows += row;
    }
    assert_eq!(3, rows);
    Ok(())
}
