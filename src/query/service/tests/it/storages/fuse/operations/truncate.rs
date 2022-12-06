// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use common_base::base::tokio;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use futures_util::TryStreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_truncate() -> common_exception::Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    // 1. truncate empty table
    let prev_version = table.get_table_info().ident.seq;
    let r = table.truncate(ctx.clone(), false).await;
    let table = fixture.latest_default_table().await?;
    // no side effects
    assert_eq!(prev_version, table.get_table_info().ident.seq);
    assert!(r.is_ok());

    // 2. truncate table which has data
    let num_blocks = 10;
    let rows_per_block = 3;
    let value_start_from = 1;
    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let source_plan = table.read_plan(ctx.clone(), None).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.seq;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.seq);

    // ensure data ingested
    let (stats, _) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    assert_eq!(stats.read_rows, (num_blocks * rows_per_block));

    // truncate
    let purge = false;
    let r = table.truncate(ctx.clone(), purge).await;
    assert!(r.is_ok());

    // get the latest tbl
    let prev_version = table.get_table_info().ident.seq;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.seq);
    let (stats, parts) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}
