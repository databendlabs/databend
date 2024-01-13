//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_storages_fuse::TableContext;
use databend_query::interpreters::InterpreterFactory;
use databend_query::test_kits::*;
use futures_util::TryStreamExt;

use crate::storages::fuse::utils::do_purge_test;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_optimize_purge() -> Result<()> {
    do_purge_test("test_fuse_snapshot_optimize_purge", 1, 0, 1, 1, 1).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_optimize_all() -> Result<()> {
    do_purge_test("test_fuse_snapshot_optimize_all", 1, 0, 1, 1, 1).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_optimize() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    // insert 5 times
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    // there will be 5 blocks
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None, true).await?;
    assert_eq!(parts.len(), n);

    // do compact
    let query = format!("optimize table {db_name}.{tbl_name} compact");

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;

    // `PipelineBuilder` will parallelize the table reading according to value of setting `max_threads`,
    // and `Table::read` will also try to de-queue read jobs preemptively. thus, the number of blocks
    // that `Table::append` takes are not deterministic (`append` is also executed in parallel in this case),
    // therefore, the final number of blocks varies.
    // To avoid flaky test, the value of setting `max_threads` is set to be 1, so that pipeline_builder will
    // only arrange one worker for the `ReadDataSourcePlan`.
    ctx.get_settings().set_max_threads(1)?;
    let data_stream = interpreter.execute(ctx.clone()).await?;
    let _ = data_stream.try_collect::<Vec<_>>().await;

    // verify compaction
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None, true).await?;
    // blocks are so tiny, they should be compacted into one
    assert_eq!(parts.len(), 1);

    Ok(())
}
