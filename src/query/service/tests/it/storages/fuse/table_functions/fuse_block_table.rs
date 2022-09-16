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

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::interpreters::CreateTableInterpreterV2;
use databend_query::interpreters::Interpreter;
use tokio_stream::StreamExt;

use crate::storages::fuse::table_test_fixture::*;

#[tokio::test]
async fn test_fuse_block_table() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();

    // test db & table
    let create_table_plan = fixture.default_crate_table_plan();
    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    {
        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 0     |",
            "+-------+",
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
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
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
        expects_err(
            "unsupported_table_engine",
            ErrorCode::logical_error_code(),
            output_stream.collect::<Result<Vec<DataBlock>>>().await,
        );
    }

    Ok(())
}
