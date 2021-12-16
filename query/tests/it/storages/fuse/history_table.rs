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

use std::sync::Arc;

use common_base::tokio;
use common_datablocks::assert_blocks_sorted_eq_with_name;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::*;
use common_streams::SendableDataBlockStream;
use databend_query::catalogs::Catalog;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sql::PlanParser;
use databend_query::storages::fuse::FuseTruncateHistory;
use databend_query::storages::FuseHistoryTable;
use databend_query::storages::ToReadDataSourcePlan;
use databend_query::table_functions::TableArgs;
use futures::TryStreamExt;
use walkdir::WalkDir;

use super::table_test_fixture::TestFixture;

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

    let arg_db = Expression::create_literal(DataValue::String(Some(test_db.as_bytes().to_vec())));
    expects_err(
        "bad argument (no table)",
        ErrorCode::bad_arguments_code(),
        test_drive_with_args(Some(vec![arg_db])).await,
    );

    let arg_db = Expression::create_literal(DataValue::String(Some(test_db.as_bytes().to_vec())));
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
    let arg_db = Expression::create_literal(DataValue::String(Some(db.as_bytes().to_vec())));
    let arg_tbl = Expression::create_literal(DataValue::String(Some(tbl.as_bytes().to_vec())));

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
            execute_query(qry.as_str(), ctx.clone()).await,
            expected,
        )
        .await?;
    }

    {
        let expected = vec![
            "+-----------+-------------+",
            "| row_count | block_count |",
            "+-----------+-------------+",
            "| 15        | 5           |",
            "+-----------+-------------+",
        ];
        let qry = format!(
            "select row_count, block_count from fuse_history('{}', '{}')",
            db, tbl
        );
        expects_ok(
            "check_row_and_block_count",
            execute_query(qry.as_str(), ctx.clone()).await,
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
            "| 15        | 5           |",
            "| 30        | 10          |",
            "+-----------+-------------+",
        ];
        let qry = format!(
            "select row_count, block_count from fuse_history('{}', '{}') order by row_count",
            db, tbl
        );
        expects_ok(
            "check_row_and_block_count_after_append",
            execute_query(qry.as_str(), ctx.clone()).await,
            expected,
        )
        .await?;
    }

    {
        // incompatible table engine
        let qry = format!("create table {}.in_mem (a int) engine =Memory", db);
        execute_query(qry.as_str(), ctx.clone()).await?;

        let qry = format!("select * from fuse_history('{}', '{}')", db, "in_mem");
        expects_err(
            "check_row_and_block_count_after_append",
            ErrorCode::bad_arguments_code(),
            execute_query(qry.as_str(), ctx.clone()).await,
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_fuse_history_truncate_in_drop_stmt() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();

    // test db & table
    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    // ingests some test data
    append_sample_data(10, &fixture).await?;
    let data_path = ctx.get_config().storage.disk.data_path;
    let root = data_path.as_str();

    let mut got_some_files = false;
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            got_some_files = true;
            break;
        }
    }
    assert!(got_some_files, "there should be some files");

    // let's Drop
    let qry = format!("drop table '{}'.'{}'", db, tbl);
    execute_command(qry.as_str(), ctx.clone()).await?;
    // there should be no files left on test root
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            panic!("there should be not file left")
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_fuse_history_truncate() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();

    // test db & table
    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    // func args
    let arg_db = Expression::create_literal(DataValue::String(Some(db.as_bytes().to_vec())));
    let arg_tbl = Expression::create_literal(DataValue::String(Some(tbl.as_bytes().to_vec())));

    {
        let expected = vec![
            "++", //
            "++", //
        ];

        expects_ok(
            "truncate_table_with_no_snapshot_should_not_panic",
            test_truncate_with_args_and_ctx(
                Some(vec![arg_db.clone(), arg_tbl.clone()]),
                ctx.clone(),
            )
            .await,
            expected,
        )
        .await?;
    }

    {
        append_sample_data(5, &fixture).await?;

        let expected = vec![
            "+------------------+-----------------+---------------+",
            "| snapshot_removed | segment_removed | block_removed |",
            "+------------------+-----------------+---------------+",
            "| 1                | 5               | 5             |",
            "+------------------+-----------------+---------------+",
        ];

        expects_ok(
            "truncate_table_with_no_history_data_should_not_panic",
            test_truncate_with_args_and_ctx(
                Some(vec![arg_db.clone(), arg_tbl.clone()]),
                ctx.clone(),
            )
            .await,
            expected,
        )
        .await?;
    }

    {
        // insert overwrite, 3 times:
        // 3 row and 1 block per insertion, there will be extra three snapshot added
        for _x in 0..3 {
            append_sample_data_overwrite(1, true, &fixture).await?;
        }

        // do the truncation (syntax sugar coming soon)
        let qry = format!("select * from fuse_truncate_history('{}', '{}')", db, tbl);
        execute_command(qry.as_str(), ctx.clone()).await?;

        // check the table history after truncation
        // only the last insertion should be kept (3 row, 1 block)
        //let expected = vec![
        //    "+-----------+-------------+",
        //    "| row_count | block_count |",
        //    "+-----------+-------------+",
        //    "| 3         | 1           |",
        //    "+-----------+-------------+",
        //];
        //let qry = format!(
        //    "select row_count, block_count from fuse_history('{}', '{}') order by row_count",
        //    db, tbl
        //);
        //expects_ok(
        //    "check_row_and_block_count_after_append",
        //    execute_query(qry.as_str(), ctx.clone()).await,
        //    expected,
        //)
        //.await?;
    }

    //   {
    //       // incompatible table engine
    //       let qry = format!("create table {}.in_mem (a int) engine =Memory", db);
    //       execute_query(qry.as_str(), ctx.clone()).await?;

    //       let qry = format!(
    //           "select * from fuse_truncate_history('{}', '{}')",
    //           db, "in_mem"
    //       );
    //       expects_err(
    //           "incompatible_table_engine",
    //           ErrorCode::bad_arguments_code(),
    //           execute_query(qry.as_str(), ctx.clone()).await,
    //       );
    //   }

    Ok(())
}

async fn test_drive(
    test_db: Option<&str>,
    test_tbl: Option<&str>,
) -> Result<SendableDataBlockStream> {
    let arg_db =
        Expression::create_literal(DataValue::String(test_db.map(|v| v.as_bytes().to_vec())));
    let arg_tbl =
        Expression::create_literal(DataValue::String(test_tbl.map(|v| v.as_bytes().to_vec())));
    let tbl_args = Some(vec![arg_db, arg_tbl]);
    test_drive_with_args(tbl_args).await
}

async fn test_drive_with_args(tbl_args: TableArgs) -> Result<SendableDataBlockStream> {
    let ctx = crate::tests::create_query_context()?;
    test_drive_with_args_and_ctx(tbl_args, ctx).await
}

async fn test_drive_with_args_and_ctx(
    tbl_args: TableArgs,
    ctx: std::sync::Arc<QueryContext>,
) -> Result<SendableDataBlockStream> {
    let func = FuseHistoryTable::create("system", "fuse_history", 1, tbl_args)?;
    let source_plan = func
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(Extras::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;
    func.read(ctx, &source_plan).await
}

async fn test_truncate_with_args_and_ctx(
    tbl_args: TableArgs,
    ctx: std::sync::Arc<QueryContext>,
) -> Result<SendableDataBlockStream> {
    let func = FuseTruncateHistory::create("system", "fuse_truncate_history", 1, tbl_args)?;
    let source_plan = func
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(Extras::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;
    func.read(ctx, &source_plan).await
}

fn expects_err<T>(case_name: &str, err_code: u16, res: Result<T>) {
    if let Err(err) = res {
        assert_eq!(
            err.code(),
            err_code,
            "case name {}, unexpected error: {}",
            case_name,
            err
        );
    } else {
        panic!(
            "case name {}, expecting err code {}, but got ok",
            case_name, err_code,
        );
    }
}

async fn expects_ok(
    case_name: impl AsRef<str>,
    res: Result<SendableDataBlockStream>,
    expected: Vec<&str>,
) -> Result<()> {
    match res {
        Ok(stream) => {
            let blocks: Vec<DataBlock> = stream.try_collect().await.unwrap();
            assert_blocks_sorted_eq_with_name(case_name.as_ref(), expected, &blocks)
        }
        Err(err) => {
            panic!(
                "case name {}, expecting  Ok, but got err {}",
                case_name.as_ref(),
                err,
            )
        }
    };
    Ok(())
}

async fn execute_query(query: &str, ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
    let plan = PlanParser::parse(query, ctx.clone()).await?;
    InterpreterFactory::get(ctx.clone(), plan)?
        .execute(None)
        .await
}

async fn execute_command(query: &str, ctx: Arc<QueryContext>) -> Result<()> {
    let res = execute_query(query, ctx).await?;
    res.try_collect::<Vec<DataBlock>>().await?;
    Ok(())
}

async fn append_sample_data(num_blocks: u32, fixture: &TestFixture) -> Result<()> {
    append_sample_data_overwrite(num_blocks, false, fixture).await
}

async fn append_sample_data_overwrite(
    num_blocks: u32,
    overwrite: bool,
    fixture: &TestFixture,
) -> Result<()> {
    let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;
    let ctx = fixture.ctx();
    let stream = table.append_data(ctx.clone(), stream).await?;
    table
        .commit(ctx, stream.try_collect().await?, overwrite)
        .await
}
