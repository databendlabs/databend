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

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::*;
use databend_query::interpreters::CreateTableInterpreter;
use tokio_stream::StreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;
use crate::storages::fuse::table_test_fixture::*;

#[tokio::test]
async fn test_clustering_information_table_read() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();

    // test db & table
    let create_table_plan = fixture.default_crate_table_plan();
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(None).await?;

    // func args
    let arg_db = Expression::create_literal(DataValue::String(db.as_bytes().to_vec()));
    let arg_tbl = Expression::create_literal(DataValue::String(tbl.as_bytes().to_vec()));

    {
        let expected = vec![
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| cluster_by_keys | total_block_count | total_constant_block_count | average_overlaps | average_depth | block_depth_histogram |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| (id)            | 0                 | 0                          | 0                | 0             | {}                    |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
        ];

        expects_ok(
            "empty_data_set",
            test_drive_clustering_information(
                Some(vec![arg_db.clone(), arg_tbl.clone()]),
                ctx.clone(),
            )
            .await,
            expected,
        )
        .await?;
    }

    {
        let qry = format!("insert into {}.{} values(1),(2)", db, tbl);
        execute_query(ctx.clone(), qry.as_str()).await?;
        let expected = vec![
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| cluster_by_keys | total_block_count | total_constant_block_count | average_overlaps | average_depth | block_depth_histogram |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
            "| (id)            | 1                 | 0                          | 0                | 1             | {\"00001\":1}           |",
            "+-----------------+-------------------+----------------------------+------------------+---------------+-----------------------+",
        ];

        let qry = format!("select * from clustering_information('{}', '{}')", db, tbl);

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
        execute_query(ctx.clone(), qry.as_str()).await?;

        let qry = format!(
            "select * from clustering_information('{}', '{}')",
            db, "in_mem"
        );
        let output_stream = execute_query(ctx.clone(), qry.as_str()).await?;
        expects_err(
            "unsupported_table_engine",
            ErrorCode::logical_error_code(),
            output_stream.collect::<Result<Vec<DataBlock>>>().await,
        );
    }

    Ok(())
}
