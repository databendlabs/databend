// Copyright 2021 Datafuse Labs
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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use databend_common_ast::ast::Engine;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::DropTablePlan;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::DropTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::rcte_hooks::RcteHookRegistry;
use databend_storages_common_table_meta::table::OPT_KEY_RECURSIVE_CTE;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

fn extract_u64(blocks: Vec<DataBlock>, col: usize) -> u64 {
    let block = DataBlock::concat(&blocks).expect("concat blocks");
    assert_eq!(block.num_rows(), 1, "unexpected rows: {}", block.num_rows());

    let scalar = block.get_by_offset(col).index(0).expect("scalar at row 0");

    match scalar {
        ScalarRef::Number(NumberScalar::UInt64(v)) => v,
        ScalarRef::Number(NumberScalar::UInt32(v)) => v as u64,
        ScalarRef::Number(NumberScalar::Int64(v)) => v as u64,
        other => panic!("unexpected scalar type for col {col}: {other:?}"),
    }
}

fn extract_two_u64(blocks: Vec<DataBlock>) -> (u64, u64) {
    let block = DataBlock::concat(&blocks).expect("concat blocks");
    assert_eq!(block.num_rows(), 1, "unexpected rows: {}", block.num_rows());
    assert!(
        block.num_columns() >= 2,
        "expected at least two columns, got {}",
        block.num_columns()
    );

    let first = block
        .get_by_offset(0)
        .index(0)
        .expect("scalar at row 0, col 0");
    let second = block
        .get_by_offset(1)
        .index(0)
        .expect("scalar at row 0, col 1");

    let to_u64 = |v: ScalarRef<'_>, col: usize| -> u64 {
        match v {
            ScalarRef::Number(NumberScalar::UInt64(v)) => v,
            ScalarRef::Number(NumberScalar::UInt32(v)) => v as u64,
            ScalarRef::Number(NumberScalar::Int64(v)) => v as u64,
            other => panic!("unexpected scalar type for col {col}: {other:?}"),
        }
    };

    (to_u64(first, 0), to_u64(second, 1))
}

async fn run_query_single_u64(ctx: Arc<QueryContext>, sql: &str) -> Result<u64> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = executor.execute(ctx).await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    Ok(extract_u64(blocks, 0))
}

async fn run_query_two_u64(ctx: Arc<QueryContext>, sql: &str) -> Result<(u64, u64)> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = executor.execute(ctx).await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    Ok(extract_two_u64(blocks))
}

/// Deterministically reproduce wrong results when recursive CTE internal table names are not
/// unique per recursive source instance.
///
/// This is a stable (non-flaky) repro: it targets the legacy shared internal table name layout
/// (`__rcte_<query_id>_0_lines`) between recursive step=0 and step=1, so step=1 reads no prepared
/// blocks and recursion stops early.
#[test]
fn recursive_cte_deterministic_wrong_count_repro() -> anyhow::Result<()> {
    let outer_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    outer_rt.block_on(async {
        use databend_common_base::runtime::Runtime;

        let fixture = Arc::new(TestFixture::setup().await?);

        let db = fixture.default_db_name();
        fixture
            .execute_command(&format!("create database if not exists {db}"))
            .await?;

        let runtime = Runtime::with_worker_threads(2, None)?;

        // Use the same QueryContext for running the query and applying interference, to avoid
        // uncertainty around per-context table caching.
        let ctx = fixture.new_query_ctx().await?;
        ctx.set_current_database(db.clone()).await?;
        ctx.get_settings().set_max_threads(8)?;

        // Pause the recursive CTE executor before it starts step=1 (only for this query id).
        let gate = RcteHookRegistry::global().install_pause_before_step(&ctx.get_id(), 1);

        let ctx_q = ctx.clone();
        let jh = runtime.spawn(async move {
            // This query should normally return 1000.
            // If the internal MEMORY table is recreated between step=0 and step=1,
            // step=1 reads no prepared blocks and recursion stops early => count becomes 1.
            let sql = "WITH RECURSIVE\n\
                       lines(x) AS (\n\
                         SELECT 1::UInt64\n\
                         UNION ALL\n\
                         SELECT x + 1\n\
                         FROM lines\n\
                         WHERE x < 1000\n\
                       )\n\
                       SELECT count(*) FROM lines";

            run_query_single_u64(ctx_q, sql).await
        });

        // Wait until the query reaches step=1 and is blocked.
        gate.wait_arrived_at_least(1).await;

        // Deterministic interference: drop and recreate the legacy shared internal recursive
        // MEMORY table name between
        // step=0 (write prepared blocks) and step=1 (read prepared blocks).
        //
        // Important: QueryContext caches tables for consistency within a query. To make the query observe
        // the recreated table, we also evict it from *the query's* table cache before resuming.
        let legacy_shared_name = format!("__rcte_{}_0_lines", ctx.get_id());
        let ctx_ddl = fixture.new_query_ctx().await?;
        ctx_ddl.set_current_database(db.clone()).await?;

        let drop_table_plan = DropTablePlan {
            if_exists: true,
            tenant: Tenant {
                tenant: ctx_ddl.get_tenant().tenant,
            },
            catalog: ctx_ddl.get_current_catalog(),
            database: db.clone(),
            table: legacy_shared_name.clone(),
            all: true,
        };
        let drop_table_interpreter =
            DropTableInterpreter::try_create(ctx_ddl.clone(), drop_table_plan)?;
        drop_table_interpreter.execute2().await?;

        let schema = TableSchemaRefExt::create(vec![TableField::new(
            "x",
            infer_schema_type(&DataType::Number(NumberDataType::UInt64))?,
        )]);

        let mut options = BTreeMap::new();
        options.insert(OPT_KEY_RECURSIVE_CTE.to_string(), "1".to_string());

        let create_table_plan = CreateTablePlan {
            schema,
            create_option: CreateOption::Create,
            tenant: Tenant {
                tenant: ctx_ddl.get_tenant().tenant,
            },
            catalog: ctx_ddl.get_current_catalog(),
            database: db.clone(),
            table: legacy_shared_name.clone(),
            engine: Engine::Memory,
            engine_options: Default::default(),
            table_properties: Default::default(),
            table_partition: None,
            storage_params: None,
            options,
            field_comments: vec![],
            cluster_key: None,
            as_select: None,
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        };
        let create_table_interpreter =
            CreateTableInterpreter::try_create(ctx_ddl.clone(), create_table_plan)?;
        let _ = create_table_interpreter.execute(ctx_ddl.clone()).await?;

        // Evict in the *query* context so the resumed recursive step re-fetches the table.
        ctx.evict_table_from_cache(&ctx.get_current_catalog(), &db, &legacy_shared_name)?;
        // Allow the query to continue step=1.
        gate.release(1);

        let got = jh.await.map_err(|e| ErrorCode::Internal(e.to_string()))??;

        // Without interference, this is 1000. If the bug is present, this becomes 1 (seed-only).
        if got != 1000 {
            return Err(ErrorCode::Internal(format!(
                "deterministic wrong-result repro: expected 1000, got {got}"
            )));
        }

        Ok::<(), ErrorCode>(())
    })?;

    Ok(())
}

/// Stress repro for flaky mismatch reported in issue #19498.
///
/// This test is intentionally ignored by default.
/// Run it manually:
///
/// ```bash
/// cargo test -p databend-query recursive_cte_issue_19498_stress_repro -- --ignored --nocapture
/// ```
///
/// Optional env vars:
/// - `RCTE_REPRO_ROUNDS` (default: 200)
/// - `RCTE_REPRO_CONCURRENCY` (default: 16)
/// - `RCTE_REPRO_SLEEP_MS` (default: 0)
#[test]
#[ignore = "manual stress repro for flaky CI issue #19498"]
fn recursive_cte_issue_19498_stress_repro() -> anyhow::Result<()> {
    let outer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    outer_rt.block_on(async {
        use databend_common_base::runtime::Runtime;

        let fixture = Arc::new(TestFixture::setup().await?);
        let db = fixture.default_db_name();
        fixture
            .execute_command(&format!("create database if not exists {db}"))
            .await?;
        let runtime = Runtime::with_worker_threads(8, None)?;

        let rounds = std::env::var("RCTE_REPRO_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(200);
        let concurrency = std::env::var("RCTE_REPRO_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16)
            .max(1);
        let sleep_ms = std::env::var("RCTE_REPRO_SLEEP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let sql = "WITH RECURSIVE aoc10_input(i) AS (SELECT '\\n89010123\\n78121874\\n87430965\\n96549874\\n45678903\\n32019012\\n01329801\\n10456732\\n'), \
                   lines(y, line, rest) AS (SELECT 0::UInt64, substr(i, 1, position('\\n' IN i) - 1), substr(i, position('\\n' IN i) + 1) \
                   FROM aoc10_input UNION ALL SELECT y + 1::UInt64, substr(rest, 1, position('\\n' IN rest) - 1), substr(rest, position('\\n' IN rest) + 1) \
                   FROM lines WHERE position('\\n' IN rest) > 0), \
                   field(x, y, v) AS (SELECT x::UInt64 AS x, y, (ascii(substr(line, x::Int64, 1)) - 48)::UInt64 AS v \
                   FROM (SELECT * FROM lines l WHERE line <> '') s, LATERAL generate_series(1, length(line)) g(x)), \
                   paths(x, y, v, sx, sy) AS (SELECT x, y, 9::UInt64, x, y FROM field WHERE v = 9 \
                   UNION ALL \
                   SELECT f.x, f.y, f.v, p.sx, p.sy FROM field f JOIN paths p ON f.v = p.v - 1 AND p.v > 0 \
                   AND ((f.x = p.x AND abs(f.y - p.y) = 1) OR (f.y = p.y AND abs(f.x - p.x) = 1))), \
                   results AS (SELECT * FROM paths WHERE v = 0), part1 AS (SELECT DISTINCT * FROM results) \
                   SELECT (SELECT count(*) FROM part1) AS part1, (SELECT count(*) FROM results) AS part2";

        let expected = (36_u64, 81_u64);
        let mut in_flight = futures_util::stream::FuturesUnordered::new();

        let mut launched = 0usize;
        let mut completed = 0usize;
        while launched < rounds && in_flight.len() < concurrency {
            launched += 1;
            let ctx = fixture.new_query_ctx().await?;
            ctx.set_current_database(db.clone()).await?;
            ctx.get_settings().set_max_threads(8)?;
            let sql = sql.to_string();
            in_flight.push(runtime.spawn(async move {
                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                let got = run_query_two_u64(ctx, &sql).await?;
                Ok::<(usize, (u64, u64)), ErrorCode>((launched, got))
            }));
        }

        while let Some(joined) = in_flight.next().await {
            let (idx, got) = joined.map_err(|e| ErrorCode::Internal(e.to_string()))??;
            completed += 1;
            if got != expected {
                return Err(ErrorCode::Internal(format!(
                    "reproduced issue #19498 at run #{idx} (completed={completed}): expected {:?}, got {:?}",
                    expected, got
                )));
            }

            if launched < rounds {
                launched += 1;
                let ctx = fixture.new_query_ctx().await?;
                ctx.set_current_database(db.clone()).await?;
                ctx.get_settings().set_max_threads(8)?;
                let sql = sql.to_string();
                in_flight.push(runtime.spawn(async move {
                    if sleep_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    }
                    let got = run_query_two_u64(ctx, &sql).await?;
                    Ok::<(usize, (u64, u64)), ErrorCode>((launched, got))
                }));
            }
        }

        Ok::<(), ErrorCode>(())
    })?;

    Ok(())
}

#[test]
fn recursive_cte_runtime_id_shared_across_child_contexts() -> anyhow::Result<()> {
    let outer_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    outer_rt.block_on(async {
        let fixture = Arc::new(TestFixture::setup().await?);
        let ctx = fixture.new_query_ctx().await?;

        let parent_runtime_id = ctx.get_or_create_logical_recursive_cte_runtime_id(7);
        let child_ctx = QueryContext::create_from(ctx.as_ref());
        let child_runtime_id = child_ctx.get_or_create_logical_recursive_cte_runtime_id(7);
        let different_runtime_id = child_ctx.get_or_create_logical_recursive_cte_runtime_id(8);

        if parent_runtime_id != child_runtime_id {
            return Err(ErrorCode::Internal(format!(
                "expected shared runtime id across query contexts, parent={parent_runtime_id}, child={child_runtime_id}"
            )));
        }

        if parent_runtime_id == different_runtime_id {
            return Err(ErrorCode::Internal(format!(
                "expected different logical recursive cte ids to map to different runtime ids, got {different_runtime_id}"
            )));
        }

        Ok::<(), ErrorCode>(())
    })?;

    Ok(())
}

#[test]
fn recursive_cte_reuse_with_multiple_correlated_subqueries_regression() -> anyhow::Result<()> {
    let outer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    outer_rt.block_on(async {
        let fixture = Arc::new(TestFixture::setup().await?);
        let db = fixture.default_db_name();
        fixture
            .execute_command(&format!("create database if not exists {db}"))
            .await?;

        let ctx = fixture.new_query_ctx().await?;
        ctx.set_current_database(db).await?;
        ctx.get_settings().set_max_threads(8)?;

        let sql = "WITH RECURSIVE digits(z, lp) AS (
                     SELECT '1', 1
                     UNION ALL SELECT CAST(lp + 1 AS TEXT), lp + 1 FROM digits WHERE lp < 3
                   ),
                   x(s, ind) AS (
                     SELECT '..', 1
                     UNION ALL
                     SELECT
                       substr(s, 1, ind - 1) || z || substr(s, ind + 1),
                       instr(substr(s, 1, ind - 1) || z || substr(s, ind + 1), '.')
                     FROM x, digits AS z
                     WHERE ind > 0
                       AND NOT EXISTS (
                         SELECT 1 FROM digits AS lp
                         WHERE z.z = '1' AND lp.lp = 2
                       )
                       AND NOT EXISTS (
                         SELECT 1 FROM digits AS lp2
                         WHERE z.z = '2' AND lp2.lp = 3
                       )
                   )
                   SELECT count(*) FROM x";

        let got = run_query_single_u64(ctx, sql).await?;
        if got != 3 {
            return Err(ErrorCode::Internal(format!(
                "expected 3 rows from multi-correlated recursive cte reuse query, got {got}"
            )));
        }

        Ok::<(), ErrorCode>(())
    })?;

    Ok(())
}

#[test]
fn recursive_cte_reuse_in_correlated_subquery_regression() -> anyhow::Result<()> {
    let outer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    outer_rt.block_on(async {
        let fixture = Arc::new(TestFixture::setup().await?);
        let db = fixture.default_db_name();
        fixture
            .execute_command(&format!("create database if not exists {db}"))
            .await?;

        let ctx = fixture.new_query_ctx().await?;
        ctx.set_current_database(db).await?;
        ctx.get_settings().set_max_threads(8)?;

        let sql = "WITH RECURSIVE digits(z, lp) AS (
                     SELECT '1', 1
                     UNION ALL SELECT CAST(lp + 1 AS TEXT), lp + 1 FROM digits WHERE lp < 3
                   ),
                   x(s, ind) AS (
                     SELECT '..', 1
                     UNION ALL
                     SELECT
                       substr(s, 1, ind - 1) || z || substr(s, ind + 1),
                       instr(substr(s, 1, ind - 1) || z || substr(s, ind + 1), '.')
                     FROM x, digits AS z
                     WHERE ind > 0
                       AND NOT EXISTS (
                         SELECT 1 FROM digits AS lp
                         WHERE z.z = '1' AND lp.lp = 2
                       )
                   )
                   SELECT count(*) FROM x";

        let got = run_query_single_u64(ctx, sql).await?;
        if got != 7 {
            return Err(ErrorCode::Internal(format!(
                "expected 7 rows from recursive cte reuse query, got {got}"
            )));
        }

        Ok::<(), ErrorCode>(())
    })?;

    Ok(())
}
