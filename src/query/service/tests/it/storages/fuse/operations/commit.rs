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
use common_base::base::tokio;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_occ_retry() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    // insert one row `id = 1` into the table, without committing
    {
        let num_blocks = 1;
        let rows_per_block = 1;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, false)
            .await?;
    }

    // insert another row `id = 5` into the table, and do commit the insertion
    {
        let num_blocks = 1;
        let rows_per_block = 1;
        let value_start_from = 5;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    // let's check it out
    let qry = format!("select * from {}.{} order by id ", db, tbl);
    let blocks = execute_query(ctx.clone(), qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;

    let expected = vec![
        "+----+----------+", //
        "| id | t        |", //
        "+----+----------+", //
        "| 1  | (2, 3)   |", //
        "| 5  | (10, 15) |", //
        "+----+----------+", //
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_last_snapshot_hint() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    let num_blocks = 1;
    let rows_per_block = 1;
    let value_start_from = 1;
    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // check last snapshot hit file
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let last_snapshot_location = fuse_table.snapshot_loc().unwrap();
    let operator = ctx.get_storage_operator()?;
    let location = fuse_table
        .meta_location_generator()
        .gen_last_snapshot_hint_location();
    let storage_meta_data = operator.metadata();
    let storage_prefix = storage_meta_data.root();

    let expected = format!("{}{}", storage_prefix, last_snapshot_location);
    let content = operator.object(location.as_str()).read().await?;

    assert_eq!(content.as_slice(), expected.as_bytes());

    Ok(())
}
