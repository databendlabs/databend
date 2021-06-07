// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_clusters_table() -> Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;

    use crate::datasources::system::*;
    use crate::datasources::*;

    let ctx = crate::tests::try_create_context()?;
    let table = ClustersTable::create();
    table.read_plan(
        ctx.clone(),
        &ScanPlan::empty(),
        ctx.get_max_threads()? as usize,
    )?;

    let stream = table.read(ctx).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    Ok(())
}
