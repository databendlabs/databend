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
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use tokio_stream::StreamExt;

use crate::storages::fuse::table_test_fixture::*;

#[tokio::test]
async fn test_fuse_snapshot_table_args() -> Result<()> {
    let test_db = "db_not_exist";
    let test_tbl = "tbl_not_exist";
    let (_guard, query_ctx) = crate::tests::create_query_context().await?;
    expects_err(
        "db_not_exist",
        ErrorCode::UNKNOWN_DATABASE,
        test_drive(query_ctx.clone(), Some(test_db), Some(test_tbl)).await,
    );

    expects_err(
        "table_not_exist",
        ErrorCode::UNKNOWN_TABLE,
        test_drive(query_ctx.clone(), Some("default"), Some(test_tbl)).await,
    );

    expects_err(
        "bad argument (None)",
        ErrorCode::BAD_ARGUMENTS,
        test_drive_with_args(query_ctx.clone(), None).await,
    );

    expects_err(
        "bad argument (empty arg vec)",
        ErrorCode::BAD_ARGUMENTS,
        test_drive_with_args(query_ctx.clone(), Some(vec![])).await,
    );

    let arg_db = DataValue::String(test_db.as_bytes().to_vec());
    expects_err(
        "bad argument (no table)",
        ErrorCode::BAD_ARGUMENTS,
        test_drive_with_args(query_ctx.clone(), Some(vec![arg_db])).await,
    );

    let arg_db = DataValue::String(test_db.as_bytes().to_vec());
    expects_err(
        "bad argument (too many args)",
        ErrorCode::BAD_ARGUMENTS,
        test_drive_with_args(
            query_ctx.clone(),
            Some(vec![arg_db.clone(), arg_db.clone(), arg_db]),
        )
        .await,
    );

    Ok(())
}

#[tokio::test]
async fn test_fuse_snapshot_table_read() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();

    // test db & table
    fixture.create_default_table().await?;

    {
        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 0     |",
            "+-------+",
        ];
        let qry = format!(
            "select count(1) as count from fuse_snapshot('{}', '{}')",
            db, tbl
        );

        expects_ok(
            "count_should_be_0",
            execute_query(ctx.clone(), qry.as_str()).await,
            expected,
        )
        .await?;
    }

    {
        // insert 5 blocks, 3 rows per block
        append_sample_data(5, &fixture).await?;
        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 1     |",
            "+-------+",
        ];
        let qry = format!(
            "select count(1) as count from fuse_snapshot('{}', '{}')",
            db, tbl
        );

        expects_ok(
            "count_should_be_1",
            execute_query(ctx.clone(), qry.as_str()).await,
            expected,
        )
        .await?;
    }

    {
        let expected = vec![
            "+-----------+-------------+",
            "| row_count | block_count |",
            "+-----------+-------------+",
            "| 15        | 1           |",
            "+-----------+-------------+",
        ];
        let qry = format!(
            "select row_count, block_count from fuse_snapshot('{}', '{}')",
            db, tbl
        );
        expects_ok(
            "check_row_and_block_count",
            execute_query(ctx.clone(), qry.as_str()).await,
            expected,
        )
        .await?;
    }

    {
        // previously, inserted 5 blocks, 3 rows per block
        // another 5 blocks, 15 rows here
        append_sample_data(5, &fixture).await?;
        let expected = vec![
            "+-----------+-------------+",
            "| row_count | block_count |",
            "+-----------+-------------+",
            "| 15        | 1           |",
            "| 30        | 2           |",
            "+-----------+-------------+",
        ];
        let qry = format!(
            "select row_count, block_count from fuse_snapshot('{}', '{}') order by row_count",
            db, tbl
        );
        expects_ok(
            "check_row_and_block_count_after_append",
            execute_query(ctx.clone(), qry.as_str()).await,
            expected,
        )
        .await?;
    }

    {
        // incompatible table engine
        let qry = format!("create table {}.in_mem (a int) engine =Memory", db);
        execute_query(ctx.clone(), qry.as_str()).await?;

        let qry = format!("select * from fuse_snapshot('{}', '{}')", db, "in_mem");
        let output_stream = execute_query(ctx.clone(), qry.as_str()).await?;
        // TODO(xuanwo): assign a new error code
        expects_err(
            "unsupported_table_engine",
            ErrorCode::INTERNAL,
            output_stream.collect::<Result<Vec<DataBlock>>>().await,
        );
    }

    Ok(())
}
