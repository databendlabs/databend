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

async fn run_query_single_u64(ctx: Arc<QueryContext>, sql: &str) -> Result<u64> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = executor.execute(ctx).await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    Ok(extract_u64(blocks, 0))
}

/// Deterministically reproduce *wrong results* caused by recursive CTE internal table name reuse.
///
/// This is a stable (non-flaky) repro: it forces the internal MEMORY table (`lines`) to be
/// corrupted between recursive step=0 and step=1, so step=1 reads no prepared blocks and the
/// recursion stops early.
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

        // Deterministic interference: drop and recreate the internal recursive MEMORY table between
        // step=0 (write prepared blocks) and step=1 (read prepared blocks).
        //
        // Important: QueryContext caches tables for consistency within a query. To make the query observe
        // the recreated table, we also evict it from *the query's* table cache before resuming.
        let ctx_ddl = fixture.new_query_ctx().await?;
        ctx_ddl.set_current_database(db.clone()).await?;

        let drop_table_plan = DropTablePlan {
            if_exists: true,
            tenant: Tenant {
                tenant: ctx_ddl.get_tenant().tenant,
            },
            catalog: ctx_ddl.get_current_catalog(),
            database: db.clone(),
            table: "lines".to_string(),
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
            table: "lines".to_string(),
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
        ctx.evict_table_from_cache(&ctx.get_current_catalog(), &db, "lines")?;
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
