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
//
use common_base::tokio;
use common_datablocks::DataBlock;
use common_exception::Result;
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
    let pending = {
        let num_blocks = 1;
        let rows_per_block = 1;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);
        table.append_data(ctx.clone(), stream).await?
    };

    // insert another row `id = 5` into the table, and do commit the insertion
    {
        let num_blocks = 1;
        let rows_per_block = 1;
        let value_start_from = 5;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);
        let r = table.append_data(ctx.clone(), stream).await?;
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, false)
            .await?;
    }

    // commit the previous pending insertion
    table
        .commit_insertion(ctx.clone(), pending.try_collect().await?, false)
        .await?;

    // let's check it out
    let qry = format!("select * from '{}'.'{}' order by id ", db, tbl);
    let blocks = execute_query(ctx.clone(), qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;

    let expected = vec![
        "+----+", //
        "| id |", //
        "+----+", //
        "| 1  |", //
        "| 5  |", //
        "+----+", //
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());

    Ok(())
}
