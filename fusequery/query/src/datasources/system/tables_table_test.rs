// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tables_table() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;

    use crate::datasources::system::*;
    use crate::datasources::*;

    let ctx = crate::tests::try_create_context()?;
    let table = TablesTable::create();
    table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_max_threads()? as usize
    )?;

    let stream = table.read(ctx).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);

    let expected = vec![
        "+----------+---------------+--------------------+",
        "| database | name          | engine             |",
        "+----------+---------------+--------------------+",
        "| system   | clusters      | SystemClusters     |",
        "| system   | contributors  | SystemContributors |",
        "| system   | databases     | SystemDatabases    |",
        "| system   | functions     | SystemFunctions    |",
        "| system   | numbers       | SystemNumbers      |",
        "| system   | numbers_local | SystemNumbersLocal |",
        "| system   | numbers_mt    | SystemNumbersMt    |",
        "| system   | one           | SystemOne          |",
        "| system   | settings      | SystemSettings     |",
        "| system   | tables        | SystemTables       |",
        "+----------+---------------+--------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
