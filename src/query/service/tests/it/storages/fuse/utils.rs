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

use std::str;

use common_exception::Result;

use super::table_test_fixture::append_sample_data;
use super::table_test_fixture::append_sample_data_overwrite;
use super::table_test_fixture::check_data_dir;
use super::table_test_fixture::execute_command;
use super::table_test_fixture::history_should_have_item;
use super::table_test_fixture::TestFixture;

pub enum TestTableOperation {
    Optimize(String),
    Analyze,
}

pub async fn do_insertions(fixture: &TestFixture) -> Result<()> {
    fixture.create_default_table().await?;
    // ingests 1 block, 1 segment, 1 snapshot
    append_sample_data(1, fixture).await?;
    // then, overwrite the table, new data set: 1 block, 1 segment, 1 snapshot
    append_sample_data_overwrite(1, true, fixture).await?;
    Ok(())
}

pub async fn do_purge_test(
    case_name: &str,
    operation: TestTableOperation,
    snapshot_count: u32,
    table_statistic_count: u32,
    segment_count: u32,
    block_count: u32,
    index_count: u32,
    after_compact: Option<(u32, u32, u32, u32, u32)>,
) -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let qry = match operation {
        TestTableOperation::Optimize(op) => format!("optimize table {}.{} {}", db, tbl, op),
        TestTableOperation::Analyze => format!("analyze table {}.{}", db, tbl),
    };

    // insert, and then insert overwrite (1 snapshot, 1 segment, 1 data block, 1 index block for each insertion);
    do_insertions(&fixture).await?;

    // execute the query
    let ctx = fixture.ctx();
    execute_command(ctx, &qry).await?;

    check_data_dir(
        &fixture,
        case_name,
        snapshot_count,
        table_statistic_count,
        segment_count,
        block_count,
        index_count,
        Some(()),
        None,
    )
    .await?;
    history_should_have_item(&fixture, case_name, snapshot_count).await?;

    if let Some((snapshot_count, table_statistic_count, segment_count, block_count, index_count)) =
        after_compact
    {
        let qry = format!("optimize table {}.{} all", db, tbl);
        execute_command(fixture.ctx().clone(), &qry).await?;

        check_data_dir(
            &fixture,
            case_name,
            snapshot_count,
            table_statistic_count,
            segment_count,
            block_count,
            index_count,
            Some(()),
            None,
        )
        .await?;

        history_should_have_item(&fixture, case_name, snapshot_count).await?;
    };

    Ok(())
}
