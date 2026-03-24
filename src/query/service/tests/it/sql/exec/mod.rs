// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::runtime::Runtime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_common_storages_fuse::FuseTable;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactory;
use databend_query::interpreters::OptimizeCompactBlockInterpreter;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::test_kits::*;
use futures_util::TryStreamExt;

#[test]
pub fn test_format_field_name() {
    use databend_query::sql::executor::decode_field_name;
    use databend_query::sql::executor::format_field_name;
    let display_name = "column_name123名字".to_string();
    let index = 12321;
    let field_name = format_field_name(display_name.as_str(), index);
    let (decoded_name, decoded_index) = decode_field_name(field_name.as_str()).unwrap();
    assert!(decoded_name == display_name && decoded_index == index);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_snapshot_consistency() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    let tbl = fixture.default_table_name();
    let db = fixture.default_db_name();
    fixture.create_default_table().await?;

    let db2 = db.clone();
    let tbl2 = tbl.clone();

    let runtime = Runtime::with_default_worker_threads()?;

    // 1. insert into tbl
    let mut planner = Planner::new(ctx.clone());
    let mut planner2 = Planner::new(ctx.clone());
    // generate 3 segments
    // insert 3 times
    let n = 3;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    let query_task = async move {
        // 2. test compact and select concurrency
        let query = format!(
            "select * from {}.{} join (select id,t from {}.{} as t2 where id > 1 and id < 100000)",
            db, tbl, db, tbl
        );

        // a. thread 1: read table
        let (query_plan, _) = planner.plan_sql(&query).await?;

        if let Plan::Query {
            s_expr: _s_expr,
            metadata,
            bind_context: _bind_context,
            rewrite_kind: _rewrite_kind,
            formatted_ast: _formatted_ast,
            ignore_result: _ignore_result,
        } = query_plan
        {
            let tbl_entries = {
                let meta = metadata.read();
                meta.tables().to_vec()
            };
            let mut tables = Vec::with_capacity(2);
            for entry in tbl_entries {
                if entry.name() == tbl {
                    tables.push(entry.table());
                }
            }
            assert_eq!(tables.len(), 2);
            let table0 = tables[0].clone();
            let table1 = tables[1].clone();

            let fuse_table0 = table0
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or_else(|| {
                    ErrorCode::Unimplemented(format!(
                        "table {}, engine type {}, does not support",
                        table0.name(),
                        table0.get_table_info().engine(),
                    ))
                })
                .unwrap();
            let snapshot0 = fuse_table0.read_table_snapshot().await?;

            let fuse_table1 = table1
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or_else(|| {
                    ErrorCode::Unimplemented(format!(
                        "table {}, engine type {}, does not support",
                        table1.name(),
                        table1.get_table_info().engine(),
                    ))
                })
                .unwrap();
            let snapshot1 = fuse_table1.read_table_snapshot().await?;

            let res = match (snapshot0, snapshot1) {
                (None, None) => true,
                (None, Some(_)) => false,
                (Some(_), None) => false,
                (Some(a), Some(b)) => a.segments == b.segments,
            };
            if !res {
                return Err(ErrorCode::BadArguments("snapshot consistency failed"));
            }
        } else {
            return Err(ErrorCode::BadArguments("query bad plan"));
        }

        Ok::<(), ErrorCode>(())
    };

    let query_handler = runtime.spawn(query_task);

    let compact_task = async move {
        let compact_sql = format!("optimize table {}.{} compact", db2, tbl2);
        let (compact_plan, _) = planner2.plan_sql(&compact_sql).await?;
        if let Plan::OptimizeCompactBlock { s_expr, need_purge } = compact_plan {
            let optimize_interpreter = OptimizeCompactBlockInterpreter::try_create(
                ctx.clone(),
                *s_expr.clone(),
                LockTableOption::LockWithRetry,
                need_purge,
            )?;
            let _ = optimize_interpreter.execute(ctx).await?;
        }
        Ok::<(), ErrorCode>(())
    };

    // b. thread2: optimize table
    let compact_handler = runtime.spawn(compact_task);

    query_handler.await.unwrap()?;
    compact_handler.await.unwrap()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_result_projection_schema_matches_nullable_join_outputs() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;
    let mut planner = Planner::new(ctx.clone());

    for sql in [
        "create or replace table issue_19568_t1(a int not null, b int not null)",
        "create or replace table issue_19568_t2(c int not null, d int not null)",
        "insert into issue_19568_t1 values (1, 10), (2, 20), (3, 30)",
        "insert into issue_19568_t2 values (1, 100), (4, 400)",
    ] {
        let (plan, _) = planner.plan_sql(sql).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = interpreter.execute(ctx.clone()).await?;
    }

    for query in [
        "select c from issue_19568_t1 left outer join issue_19568_t2 on issue_19568_t1.a = issue_19568_t2.c order by c nulls first",
        "select a, c from issue_19568_t1 full outer join issue_19568_t2 on issue_19568_t1.a = issue_19568_t2.c order by a nulls first, c nulls first",
        "select c from issue_19568_t1 left outer join issue_19568_t2 on issue_19568_t1.a > issue_19568_t2.c order by c nulls first",
        "select a, c from issue_19568_t1 full outer join issue_19568_t2 on issue_19568_t1.a < issue_19568_t2.c order by a nulls first, c nulls first",
    ] {
        let (plan, _) = planner.plan_sql(query).await?;
        let Plan::Query {
            s_expr,
            bind_context,
            metadata,
            ..
        } = plan
        else {
            unreachable!("expected query plan");
        };

        let mut builder = PhysicalPlanBuilder::new(metadata, ctx.clone(), false);
        let physical_plan = builder.build(&s_expr, bind_context.column_set()).await?;
        let output_schema = physical_plan.output_schema()?;

        for column in &bind_context.columns {
            let field = output_schema.field_with_name(&column.index.to_string())?;
            assert_eq!(
                field.data_type(),
                column.data_type.as_ref(),
                "query `{query}` produced mismatched result projection schema for column {}",
                column.column_name
            );
        }
    }

    Ok(())
}

mod get_table_bind_test;
