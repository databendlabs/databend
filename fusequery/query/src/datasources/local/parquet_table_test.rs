// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test]
async fn test_parquet_table() -> crate::error::FuseQueryResult<()> {
    use std::env;

    use arrow::datatypes::{Field, Schema};
    use common_datavalues::DataType;
    use common_planners::*;
    use futures::TryStreamExt;

    use crate::datasources::local::*;

    let options: TableOptions = [(
        "location".to_string(),
        env::current_dir()?
            .join("../../tests/data/alltypes_plain.parquet")
            .display()
            .to_string(),
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let table = ParquetTable::try_create(
        ctx.clone(),
        "default".into(),
        "test_parquet".into(),
        Schema::new(vec![Field::new("id", DataType::Int32, false)]).into(),
        options,
    )?;
    table.read_plan(ctx.clone(), PlanBuilder::empty().build()?)?;

    let stream = table.read(ctx).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();

    assert_eq!(rows, 8);
    Ok(())
}
