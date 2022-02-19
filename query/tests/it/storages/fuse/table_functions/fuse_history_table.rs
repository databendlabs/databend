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
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::*;
use databend_query::catalogs::Catalog;

use crate::storages::fuse::table_test_fixture::TestFixture;
use crate::storages::fuse::table_test_fixture::*;

#[tokio::test]
async fn test_fuse_history_table_args() -> Result<()> {
    let test_db = "db_not_exist";
    let test_tbl = "tbl_not_exist";
    expects_err(
        "db_not_exist",
        ErrorCode::unknown_database_code(),
        test_drive(Some(test_db), Some(test_tbl)).await,
    );

    expects_err(
        "table_not_exist",
        ErrorCode::unknown_table_code(),
        test_drive(Some("default"), Some(test_tbl)).await,
    );

    expects_err(
        "bad argument (None)",
        ErrorCode::bad_arguments_code(),
        test_drive_with_args(None).await,
    );

    expects_err(
        "bad argument (empty arg vec)",
        ErrorCode::bad_arguments_code(),
        test_drive_with_args(Some(vec![])).await,
    );

    let arg_db = Expression::create_literal(DataValue::String(test_db.as_bytes().to_vec()));
    expects_err(
        "bad argument (no table)",
        ErrorCode::bad_arguments_code(),
        test_drive_with_args(Some(vec![arg_db])).await,
    );

    let arg_db = Expression::create_literal(DataValue::String(test_db.as_bytes().to_vec()));
    expects_err(
        "bad argument (too many args)",
        ErrorCode::bad_arguments_code(),
        test_drive_with_args(Some(vec![arg_db.clone(), arg_db.clone(), arg_db])).await,
    );

    Ok(())
}

#[tokio::test]
async fn test_fuse_history_table_read() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();

    // test db & table
    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    // func args
    let arg_db = Expression::create_literal(DataValue::String(db.as_bytes().to_vec()));
    let arg_tbl = Expression::create_literal(DataValue::String(tbl.as_bytes().to_vec()));

    {
        let expected = vec![
            "+-------------+------------------+---------------+-------------+-----------+--------------------+------------------+",
            "| snapshot_id | prev_snapshot_id | segment_count | block_count | row_count | bytes_uncompressed | bytes_compressed |",
            "+-------------+------------------+---------------+-------------+-----------+--------------------+------------------+",
            "+-------------+------------------+---------------+-------------+-----------+--------------------+------------------+",
        ];

        expects_ok(
            "empty_data_set",
            test_drive_with_args_and_ctx(Some(vec![arg_db.clone(), arg_tbl.clone()]), ctx.clone())
                .await,
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
            "select count(*) as count from fuse_history('{}', '{}')",
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
            "select row_count, block_count from fuse_history('{}', '{}')",
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
            "select row_count, block_count from fuse_history('{}', '{}') order by row_count",
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

        let qry = format!("select * from fuse_history('{}', '{}')", db, "in_mem");
        expects_err(
            "check_row_and_block_count_after_append",
            ErrorCode::bad_arguments_code(),
            execute_query(ctx.clone(), qry.as_str()).await,
        );
    }

    Ok(())
}
