// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_settings_table() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::datasources::system::*;
    use crate::datasources::*;

    let ctx = crate::tests::try_create_context()?;
    let table = SettingsTable::create();
    table.read_plan(ctx.clone(), PlanBuilder::empty().build()?)?;

    let stream = table.read(ctx).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    let expected = vec![
        "+----------------+---------+---------------+---------------------------------------------------------------------------------------------------+",
        "| name           | value   | default_value | description                                                                                       |",
        "+----------------+---------+---------------+---------------------------------------------------------------------------------------------------+",
        "| default_db     | default | default       | the default database for current session                                                          |",
        "| max_block_size | 10000   | 10000         | Maximum block size for reading                                                                    |",
        "| max_threads    | 8       | 16            | The maximum number of threads to execute the request. By default, it is determined automatically. |",
        "+----------------+---------+---------------+---------------------------------------------------------------------------------------------------+",

    ];
    crate::assert_blocks_sorted_eq!(expected, result.as_slice());

    Ok(())
}
