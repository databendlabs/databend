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
use databend_query::test_kits::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_truncate_in_drop_stmt() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    // ingests some test data
    append_sample_data(10, &fixture).await?;
    // let's Drop
    let qry = format!("drop table {}.{}", db, tbl);
    fixture.execute_command(qry.as_str()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_snapshot_truncate_in_drop_all_stmt() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    // ingests some test data
    append_sample_data(1, &fixture).await?;
    // let's Drop
    let qry = format!("drop table {}.{} all", db, tbl);
    fixture.execute_command(qry.as_str()).await?;

    check_data_dir(
        &fixture,
        "drop table: there should be 1 snapshot, 0 segment/block",
        1, // 1 snapshot
        0, // 0 snapshot statistic
        0, // 0 segments
        0, // 0 blocks
        0, // 0 index
        None,
        None,
    )
    .await?;

    Ok(())
}
