// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::datasources::local::*;

#[tokio::test]
async fn test_parquet_table() -> Result<()> {
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
        "default".into(),
        "test_parquet".into(),
        DataSchemaRefExt::create(vec![DataField::new("id", DataType::Int32, false)]).clone(),
        options,
    )?;

    let source_plan = table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_max_threads()? as usize,
    )?;

    let stream = table.read(ctx, &source_plan).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();

    assert_eq!(rows, 8);
    Ok(())
}
