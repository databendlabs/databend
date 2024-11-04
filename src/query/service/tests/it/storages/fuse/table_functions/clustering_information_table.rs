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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::test_kits::*;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_clustering_information_table_read() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.new_query_ctx().await?;

    // test db & table
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    {
        let qry = format!("insert into {}.{} values(1, (2, 3)),(2, (4, 6))", db, tbl);
        let _ = execute_query(ctx.clone(), qry.as_str()).await?;
        let expected = vec![
            "+----------+----------+----------+----------+----------+----------+---------------+",
            "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6      |",
            "+----------+----------+----------+----------+----------+----------+---------------+",
            "| '(id)'   | 'linear' | 1        | 0        | 0        | 1        | '{\"00001\":1}' |",
            "+----------+----------+----------+----------+----------+----------+---------------+",
        ];

        let qry = format!(
            "select * exclude(timestamp) from clustering_information('{}', '{}')",
            db, tbl
        );

        expects_ok(
            "clustering_information",
            execute_query(ctx.clone(), qry.as_str()).await,
            expected,
        )
        .await?;
    }

    {
        // incompatible table engine
        let qry = format!("create table {}.in_mem (a int) engine =Memory", db);
        let _ = execute_query(ctx.clone(), qry.as_str()).await?;

        let qry = format!(
            "select * from clustering_information('{}', '{}')",
            db, "in_mem"
        );
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
