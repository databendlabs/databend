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
use common_exception::Result;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::append_sample_data;
use crate::storages::fuse::table_test_fixture::append_sample_data_overwrite;
use crate::storages::fuse::table_test_fixture::check_data_dir;
use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::expects_ok;
use crate::storages::fuse::table_test_fixture::history_should_have_only_one_item;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_history_optimize() -> Result<()> {
    do_purge_test("implicit pure", "").await
}

#[tokio::test]
async fn test_fuse_history_optimize_purge() -> Result<()> {
    do_purge_test("explicit pure", "purge").await
}

#[tokio::test]
async fn test_fuse_history_optimize_all() -> Result<()> {
    do_purge_test("explicit pure", "all").await
}

async fn do_purge_test(case_name: &str, operation: &str) -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let qry = format!("optimize table '{}'.'{}' {}", db, tbl, operation);
    // insert, and then insert overwrite (1 snapshot, 1 segment, 1 block for each insertion);
    insert_test_data(&qry, &fixture).await?;
    // there should be only 1 snapshot, 1 segment, 1 block left
    check_data_dir(&fixture, case_name, 1, 1, 1).await;
    history_should_have_only_one_item(&fixture, case_name).await
}

async fn insert_test_data(qry: &str, fixture: &TestFixture) -> Result<()> {
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;
    // ingests 1 block, 1 segment, 1 snapshot
    append_sample_data(1, fixture).await?;
    // then, overwrite the table, new data set: 1 block, 1 segment, 1 snapshot
    append_sample_data_overwrite(1, true, fixture).await?;
    execute_command(ctx.clone(), qry).await?;
    Ok(())
}

#[tokio::test]
async fn test_fuse_history_optimize_compact() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // insert 5 blocks
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);
        let r = table.append_data(ctx.clone(), stream).await?;
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, false)
            .await?;
    }

    // optimize compact
    let qry = format!("optimize table '{}'.'{}' compact", db, tbl);
    execute_command(fixture.ctx(), qry.as_str()).await?;

    // optimize compact should keep the histories
    // there should be 6 history items there, 5 for the above insertions, 1 for that compaction
    let expected = vec![
        "+----------+",
        "| count(0) |",
        "+----------+",
        "| 6        |",
        "+----------+",
    ];
    let qry = format!("select count(*) from fuse_history('{}', '{}')", db, tbl);

    expects_ok(
        "count_should_be_1",
        execute_query(fixture.ctx(), qry.as_str()).await,
        expected,
    )
    .await
}
