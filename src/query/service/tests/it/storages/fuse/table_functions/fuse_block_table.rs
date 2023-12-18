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
use databend_common_expression::DataBlock;
use databend_query::test_kits::*;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_block_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.new_query_ctx().await?;

    // test db & table
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    {
        let expected = vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 0        |",
            "+----------+",
        ];
        let qry = format!(
            "select count(1) as count from fuse_block('{}', '{}')",
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
        let qry = format!("insert into {}.{} values(1, (2, 3)),(2, (4, 6))", db, tbl);
        execute_query(ctx.clone(), qry.as_str()).await?;
        let qry = format!("insert into {}.{} values(7, (8, 9))", db, tbl);
        execute_query(ctx.clone(), qry.as_str()).await?;
        let expected = vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 2        |",
            "+----------+",
        ];

        let qry = format!(
            "select count(1) as count from fuse_block('{}', '{}')",
            db, tbl
        );

        expects_ok(
            "count_should_be_2",
            execute_query(ctx.clone(), qry.as_str()).await,
            expected,
        )
        .await?;
    }

    {
        // incompatible table engine
        let qry = format!("create table {}.in_mem (a int) engine =Memory", db);
        execute_query(ctx.clone(), qry.as_str()).await?;

        let qry = format!("select * from fuse_block('{}', '{}')", db, "in_mem");
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
