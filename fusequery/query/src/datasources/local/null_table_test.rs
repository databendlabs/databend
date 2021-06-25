// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::*;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::datasources::local::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_null_table() -> anyhow::Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let table = NullTable::try_create(
        "default".into(),
        "a".into(),
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::UInt64, false)]).into(),
        TableOptions::default(),
    )?;

    let source_plan = table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_max_threads()? as usize,
    )?;
    assert_eq!(table.engine(), "Null");

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    Ok(())
}
