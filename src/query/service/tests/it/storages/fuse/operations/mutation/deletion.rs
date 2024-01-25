//  Copyright 2022 Datafuse Labs.
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
async fn test_deletion_mutator_multiple_empty_segments() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    // insert
    for i in 0..10 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tbl_name, i);
        fixture.execute_command(qry.as_str()).await?;
    }

    // delete
    let query = format!("delete from {}.{} where id=1", db_name, tbl_name);
    fixture.execute_command(&query).await?;

    // check count
    let expected = vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 9        | 9        |",
        "+----------+----------+",
    ];
    let qry = format!(
        "select segment_count, block_count as count from fuse_snapshot('{}', '{}') limit 1",
        db_name, tbl_name
    );
    expects_ok(
        "check segment and block count",
        fixture.execute_query(qry.as_str()).await,
        expected,
    )
    .await?;

    Ok(())
}
