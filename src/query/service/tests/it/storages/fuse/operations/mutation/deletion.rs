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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::NumberScalar;
use databend_query::test_kits::*;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_deletion_mutator_multiple_empty_segments() -> anyhow::Result<()> {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_by_block_name() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    fixture
        .execute_command(&format!("insert into {db}.{tbl} values(1, (2, 3))"))
        .await?;

    let blocks = fixture
        .execute_query(&format!("select _block_name from {db}.{tbl} limit 1"))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let block_name = match blocks.first().and_then(|b| b.get_by_offset(0).index(0)) {
        Some(ScalarRef::String(s)) => s.to_string(),
        Some(other) => {
            return Err(ErrorCode::Internal(format!(
                "unexpected _block_name scalar: {other:?}"
            )));
        }
        None => {
            return Err(ErrorCode::Internal(
                "failed to fetch _block_name".to_string(),
            ));
        }
    };

    fixture
        .execute_command(&format!(
            "delete from {db}.{tbl} where _block_name = '{block_name}'"
        ))
        .await?;

    let count_blocks = fixture
        .execute_query(&format!("select count() from {db}.{tbl}"))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let count = match count_blocks
        .first()
        .and_then(|b| b.get_by_offset(0).index(0))
    {
        Some(ScalarRef::Number(NumberScalar::UInt64(v))) => v,
        Some(other) => {
            return Err(ErrorCode::Internal(format!(
                "unexpected count() scalar: {other:?}"
            )));
        }
        None => return Err(ErrorCode::Internal("failed to fetch count()".to_string())),
    };
    assert_eq!(count, 0);

    Ok(())
}
