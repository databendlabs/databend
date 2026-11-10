// Copyright 2026 Datafuse Labs.
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

use databend_common_catalog::session_type::SessionType;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::NumberScalar;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_meta_client::types::MatchSeq;
use databend_query::interpreters::InterpreterFactory;
use databend_query::schedulers::ServiceQueryExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use databend_query::test_kits::execute_query;
use databend_storages_common_table_meta::table::OPT_KEY_AS_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_QUERY_CATALOG;
use databend_storages_common_table_meta::table::OPT_KEY_QUERY_DATABASE;
use futures::TryStreamExt;

// Register exactly the old VIEW metadata shape: a qualified definition with no
// namespace options. CREATE VIEW itself must always write the new shape.
async fn register_legacy_view(ctx: &Arc<QueryContext>, name: &str, query: &str) -> Result<()> {
    ctx.get_catalog("default")
        .await?
        .create_table(CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant: ctx.get_tenant(),
                db_name: "legacy_cte".to_string(),
                table_name: name.to_string(),
            },
            table_meta: TableMeta {
                engine: VIEW_ENGINE.to_string(),
                options: [(QUERY.to_string(), query.to_string())].into(),
                ..Default::default()
            },
            source_table_option: None,
            as_dropped: false,
            materialized_view: None,
            clone: None,
            table_properties: None,
            table_partition: None,
        })
        .await?;
    ctx.clear_tables_cache();
    Ok(())
}

async fn assert_numbers(ctx: &Arc<QueryContext>, sql: &str, expected: &[&[i32]]) -> Result<()> {
    ctx.clear_tables_cache();
    ctx.clear_table_meta_timestamps_cache();
    let blocks = execute_query(ctx.clone(), sql)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = DataBlock::concat(&blocks)?;
    let actual = (0..block.num_rows())
        .map(|row| {
            block
                .columns()
                .iter()
                .map(|column| column.index(row).unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|n| Scalar::Number(NumberScalar::Int32(*n)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{sql}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_view_cte_resolution_is_scoped() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    execute_command(ctx.clone(), "CREATE DATABASE legacy_cte").await?;
    execute_command(ctx.clone(), "CREATE TABLE legacy_cte.t(n INT)").await?;
    execute_command(ctx.clone(), "INSERT INTO legacy_cte.t VALUES (1)").await?;

    let old_sql = "WITH t AS (SELECT 99::INT AS n) SELECT * FROM legacy_cte.t";
    register_legacy_view(&ctx, "v_old", old_sql).await?;
    register_legacy_view(
        &ctx,
        "v_missing",
        "WITH missing AS (SELECT 98::INT AS n) SELECT * FROM legacy_cte.missing",
    )
    .await?;
    register_legacy_view(
        &ctx,
        "v_scalar",
        "WITH t AS (SELECT 97::INT AS n) SELECT (SELECT n FROM legacy_cte.t) AS n",
    )
    .await?;
    register_legacy_view(
        &ctx,
        "v_repeated",
        "WITH t AS (SELECT 99::INT AS n) SELECT (a.n + b.n)::INT AS n FROM legacy_cte.t a, legacy_cte.t b",
    )
    .await?;

    register_legacy_view(
        &ctx,
        "v_catalog",
        "WITH t AS (SELECT 96::INT AS n) SELECT * FROM default.legacy_cte.t",
    )
    .await?;
    for sql in [
        "CREATE TABLE legacy_cte.events(v VARIANT) ENGINE = MEMORY",
        "INSERT INTO legacy_cte.events VALUES (parse_json('{\"message\":{\"level\":7}}'))",
        "CREATE TABLE legacy_cte.logs(message VARIANT) ENGINE = MEMORY",
        "INSERT INTO legacy_cte.logs VALUES (parse_json('{\"level\":100}'))",
    ] {
        execute_command(ctx.clone(), sql).await?;
    }
    register_legacy_view(
        &ctx,
        "v_json",
        "WITH logs AS (SELECT v['message'] AS message FROM legacy_cte.events) SELECT (l.message['level']::INT + r.message['level']::INT)::INT AS n FROM legacy_cte.logs l, legacy_cte.logs r",
    )
    .await?;
    ctx.get_settings()
        .set_enable_experimental_virtual_column(1)?;

    // Only repeated consumers change binding paths when auto-materialization is
    // toggled. Single-consumer views and scope restoration need one check each.
    for auto_materialize in [0, 1] {
        ctx.get_settings()
            .set_enable_auto_materialize_cte(auto_materialize)?;
        assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_repeated", &[&[198]]).await?;
        assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_json", &[&[14]]).await?;
    }
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_old", &[&[99]]).await?;
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_missing", &[&[98]]).await?;
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_scalar", &[&[97]]).await?;
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_catalog", &[&[96]]).await?;

    // A qualified name is strict outside the legacy view, in either join order
    // and in scalar subqueries created after that view has been bound.
    assert_numbers(
        &ctx,
        "WITH t AS (SELECT 77::INT AS n) SELECT v.n, r.n, (SELECT n FROM legacy_cte.t) FROM legacy_cte.v_old v, legacy_cte.t r",
        &[&[99, 1, 1]],
    )
    .await?;
    assert_numbers(
        &ctx,
        "WITH t AS (SELECT 77::INT AS n) SELECT r.n, v.n FROM legacy_cte.t r, legacy_cte.v_old v",
        &[&[1, 99]],
    )
    .await?;

    // The same SQL is strict as an ordinary query and in a newly created view.
    assert_numbers(&ctx, old_sql, &[&[1]]).await?;
    execute_command(
        ctx.clone(),
        &format!("CREATE VIEW legacy_cte.v_new AS {old_sql}"),
    )
    .await?;
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_new", &[&[1]]).await?;
    execute_command(
        ctx.clone(),
        "CREATE VIEW legacy_cte.v_mixed AS WITH t AS (SELECT 77::INT AS n) SELECT v.n AS x, r.n AS y FROM legacy_cte.v_old v, legacy_cte.t r",
    )
    .await?;
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_mixed", &[&[99, 1]]).await?;
    register_legacy_view(
        &ctx,
        "v_wrap_new",
        "WITH t AS (SELECT 77::INT AS n) SELECT v.n AS x, (SELECT n FROM legacy_cte.t) AS y FROM legacy_cte.v_new v",
    )
    .await?;
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_wrap_new", &[&[1, 77]]).await?;

    // SHOW CREATE retains the stored legacy text; recreating that explicitly
    // qualified SQL adopts the strict rule. Only the original unqualified SQL
    // can unambiguously express that the user intended a CTE instead of a table.
    let blocks = execute_query(ctx.clone(), "SHOW CREATE TABLE legacy_cte.v_old")
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = DataBlock::concat(&blocks)?;
    let Some(ScalarRef::String(ddl)) = block.get_by_offset(1).index(0) else {
        panic!("SHOW CREATE should return the stored definition");
    };
    assert!(ddl.contains("FROM legacy_cte.t"));
    execute_command(
        ctx.clone(),
        &ddl.replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1),
    )
    .await?;
    ctx.clear_tables_cache();
    let view = ctx
        .get_catalog("default")
        .await?
        .get_table(&ctx.get_tenant(), "legacy_cte", "v_old")
        .await?;
    assert_eq!(
        view.options()
            .get(OPT_KEY_QUERY_DATABASE)
            .map(String::as_str),
        Some("legacy_cte")
    );
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_old", &[&[1]]).await?;

    // Rebuilding from original unqualified CTE SQL retains the intended CTE result.
    execute_command(
        ctx.clone(),
        "CREATE OR REPLACE VIEW legacy_cte.v_missing AS WITH missing AS (SELECT 98::INT AS n) SELECT * FROM missing",
    )
    .await?;
    ctx.clear_tables_cache();
    assert_numbers(&ctx, "SELECT * FROM legacy_cte.v_missing", &[&[98]]).await?;
    Ok(())
}

// Scalar subqueries are bound by binders that the type checker creates from a
// `BindContext`, so they only see the view's definition scope through that
// context. Cover each place that builds the context for such a subquery.
const SUBQUERY_POSITIONS: &[(&str, &str, i32)] = &[
    ("select", "SELECT (SELECT n FROM {c}) AS n", 7),
    (
        "where",
        "SELECT n FROM {base} WHERE n < (SELECT n FROM {c})",
        1,
    ),
    (
        "join_on",
        "SELECT x.n FROM {base} x JOIN {base} y ON x.n + 6 = (SELECT n FROM {c})",
        1,
    ),
    (
        "having",
        "SELECT count(*)::INT AS n FROM {base} HAVING count(*) < (SELECT n FROM {c})",
        1,
    ),
    (
        "qualify",
        "SELECT n FROM {base} QUALIFY row_number() OVER (ORDER BY n) < (SELECT n FROM {c})",
        1,
    ),
    (
        "order_by",
        "SELECT n FROM {base} ORDER BY (SELECT n FROM {c})",
        1,
    ),
    (
        "in",
        "SELECT n FROM {base} WHERE n + 6 IN (SELECT n FROM {c})",
        1,
    ),
    (
        "exists",
        "SELECT n FROM {base} WHERE EXISTS (SELECT 1 FROM {c})",
        1,
    ),
    (
        "union",
        "SELECT n FROM {base} WHERE n > 1 UNION ALL SELECT (SELECT n FROM {c})",
        7,
    ),
    (
        "lateral",
        "SELECT l.n FROM {base} b, LATERAL (SELECT n FROM {c} WHERE n > b.n) l",
        7,
    ),
    ("nested", "SELECT (SELECT (SELECT n FROM {c}) AS n) AS n", 7),
    (
        "derived",
        "SELECT (SELECT max(n) FROM (SELECT n FROM {c}) s) AS n",
        7,
    ),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_view_scope_reaches_scalar_subqueries() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    for sql in [
        "CREATE DATABASE legacy_cte",
        "CREATE TABLE legacy_cte.base(n INT)",
        "INSERT INTO legacy_cte.base VALUES (1)",
        "CREATE DATABASE scope_ns",
        "CREATE TABLE scope_ns.base(n INT)",
        "INSERT INTO scope_ns.base VALUES (1)",
        "CREATE TABLE scope_ns.c(n INT)",
        "INSERT INTO scope_ns.c VALUES (7)",
    ] {
        execute_command(ctx.clone(), sql).await?;
    }
    // Query from `default`, where neither `base` nor `c` exists, so falling back
    // to the session scope fails instead of returning a plausible value.
    assert_eq!(ctx.get_current_database(), "default");

    for (name, body, expected) in SUBQUERY_POSITIONS {
        // Legacy: `legacy_cte.c` only names the CTE, so strict resolution would
        // look for a nonexistent table.
        let view = format!("v_{name}");
        let legacy = body
            .replace("{base}", "legacy_cte.base")
            .replace("{c}", "legacy_cte.c");
        register_legacy_view(
            &ctx,
            &view,
            &format!("WITH c AS (SELECT 7::INT AS n) {legacy}"),
        )
        .await?;
        let sql = format!("SELECT * FROM legacy_cte.{view}");
        if let Err(e) = assert_numbers(&ctx, &sql, &[&[*expected]]).await {
            panic!("{sql}: {e}");
        }

        // Stored namespace: unqualified tables resolve in the view's database.
        let strict = body.replace("{base}", "base").replace("{c}", "c");
        execute_command(
            ctx.clone(),
            &format!("CREATE VIEW scope_ns.{view} AS {strict}"),
        )
        .await?;
        let sql = format!("SELECT * FROM scope_ns.{view}");
        if let Err(e) = assert_numbers(&ctx, &sql, &[&[*expected]]).await {
            panic!("{sql}: {e}");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_view_schema_readers() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    execute_command(ctx.clone(), "CREATE DATABASE legacy_cte").await?;
    register_legacy_view(
        &ctx,
        "v_missing",
        "WITH missing AS (SELECT 98::INT AS n) SELECT * FROM legacy_cte.missing",
    )
    .await?;
    let blocks = execute_query(ctx.clone(), "DESC legacy_cte.v_missing")
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let description = DataBlock::concat(&blocks)?;
    assert_eq!(description.num_rows(), 1);
    assert_eq!(
        description.get_by_offset(0).index(0),
        Some(ScalarRef::String("n"))
    );
    assert_numbers(
        &ctx,
        "SELECT count(*)::INT FROM system.columns WHERE database = 'legacy_cte' AND table = 'v_missing'",
        &[&[1]],
    ).await?;
    execute_command(
        ctx.clone(),
        "CREATE TABLE legacy_cte.copied LIKE legacy_cte.v_missing",
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_recursive_view_cte() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    execute_command(ctx.clone(), "CREATE DATABASE legacy_cte").await?;
    register_legacy_view(
        &ctx,
        "v_recursive",
        "WITH RECURSIVE r(n) AS (SELECT 1::INT UNION ALL SELECT (n + 1)::INT FROM legacy_cte.r WHERE n < 3) SELECT * FROM legacy_cte.r",
    )
    .await?;
    assert_numbers(
        &ctx,
        "SELECT n::INT FROM legacy_cte.v_recursive ORDER BY n",
        &[&[1], &[2], &[3]],
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_materialized_cte_producer_context() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    execute_command(ctx.clone(), "CREATE DATABASE legacy_cte").await?;
    // The producer for b retains the lazy a in its WITH clause and still
    // references it as legacy_cte.a, so strict producer binding would look for a
    // nonexistent table.
    register_legacy_view(
        &ctx,
        "v_materialized",
        "WITH a AS (SELECT 99::INT AS n), b AS MATERIALIZED (SELECT n FROM legacy_cte.a) SELECT n FROM legacy_cte.b",
    )
    .await?;
    // The producer for b drops the user-materialized a from its WITH clause, so
    // legacy_cte.a must be replaced with a's temporary table even though that
    // table lives in the session database, not in legacy_cte.
    register_legacy_view(
        &ctx,
        "v_materialized_chain",
        "WITH a AS MATERIALIZED (SELECT 98::INT AS n), b AS MATERIALIZED (SELECT legacy_cte.a.n FROM legacy_cte.a) SELECT n FROM legacy_cte.b",
    )
    .await?;

    // Use the production executor to verify that the separately planned CTAS
    // producers inherit the old resolution policy as well as the main query.
    // Keep the session in `default`: legacy definitions qualify CTE references
    // with the view's database, which must not have to match the session's.
    for (view, expected) in [("v_materialized", 99), ("v_materialized_chain", 98)] {
        let session = fixture.new_session_with_type(SessionType::MySQL).await?;
        let query_ctx = session
            .create_query_context(&databend_common_version::BUILD_INFO)
            .await?;
        assert_eq!(query_ctx.get_current_database(), "default");
        let mut planner = Planner::new_with_query_executor(
            query_ctx.clone(),
            Arc::new(ServiceQueryExecutor::new(query_ctx.clone())),
        );
        let (plan, _) = planner
            .plan_sql(&format!("SELECT * FROM legacy_cte.{view}"))
            .await?;
        let interpreter = InterpreterFactory::get(query_ctx.clone(), &plan).await?;
        let blocks = interpreter
            .execute(query_ctx)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        let block = DataBlock::concat(&blocks)?;
        assert_eq!(block.num_rows(), 1, "{view}");
        assert_eq!(
            block.get_by_offset(0).index(0),
            Some(ScalarRef::Number(NumberScalar::Int32(expected))),
            "{view}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_definition_cache_isolation() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    execute_command(ctx.clone(), "CREATE DATABASE legacy_cte_cache").await?;
    execute_command(ctx.clone(), "CREATE TABLE legacy_cte_cache.t(n INT)").await?;
    execute_command(ctx.clone(), "INSERT INTO legacy_cte_cache.t VALUES (1)").await?;
    ctx.get_settings()
        .set_setting("enable_planner_cache".to_string(), "1".to_string())?;
    ctx.get_settings()
        .set_setting("enable_query_result_cache".to_string(), "1".to_string())?;

    let sql = "WITH t AS (SELECT 99::INT AS n) SELECT * FROM legacy_cte_cache.t";
    let mut strict = Planner::new(ctx.clone());
    let (plan, _) = strict.plan_sql(sql).await?;
    let Plan::Query {
        metadata,
        formatted_ast,
        ..
    } = plan
    else {
        panic!("expected a query");
    };
    assert!(formatted_ast.is_some());
    assert!(
        metadata
            .read()
            .tables()
            .iter()
            .any(|table| table.name() == "t")
    );

    let mut legacy = Planner::new(ctx.clone()).with_stored_definition(&BTreeMap::new());
    let (plan, _) = legacy.plan_sql(sql).await?;
    let Plan::Query {
        metadata,
        formatted_ast,
        ..
    } = plan
    else {
        panic!("expected a query");
    };
    assert!(formatted_ast.is_none());
    assert!(
        !metadata
            .read()
            .tables()
            .iter()
            .any(|table| table.name() == "t")
    );

    let mut strict_again = Planner::new(ctx.clone());
    let (plan, _) = strict_again.plan_sql(sql).await?;
    assert!(matches!(plan, Plan::Query {
        formatted_ast: Some(_),
        ..
    }));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_dynamic_table_refresh_uses_legacy_ctes() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    for sql in [
        "CREATE DATABASE legacy_dynamic_cte",
        "USE legacy_dynamic_cte",
        "CREATE TABLE src(n INT)",
        "INSERT INTO src VALUES (1)",
        "CREATE TABLE t(n INT)",
        "INSERT INTO t VALUES (900)",
        "CREATE DYNAMIC TABLE dt AS WITH t AS (SELECT * FROM legacy_dynamic_cte.src) SELECT * FROM t",
    ] {
        fixture.execute_command(sql).await?;
    }
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(&ctx.get_tenant(), "legacy_dynamic_cte", "dt")
        .await?;
    // Reconstruct the older writer's metadata: all table/CTE references have a
    // database and no source namespace was recorded. Keep its schema and source ids.
    catalog.upsert_table_option(
        &ctx.get_tenant(),
        "legacy_dynamic_cte",
        UpsertTableOptionReq {
            table_id: table.get_id(),
            seq: MatchSeq::Exact(table.get_table_info().ident.seq),
            options: [
                (OPT_KEY_QUERY_CATALOG.to_string(), None),
                (OPT_KEY_QUERY_DATABASE.to_string(), None),
                (OPT_KEY_AS_QUERY.to_string(), Some("WITH t AS (SELECT * FROM legacy_dynamic_cte.src) SELECT * FROM legacy_dynamic_cte.t".to_string())),
            ].into(),
        },
    ).await?;
    ctx.clear_tables_cache();
    fixture
        .execute_command("INSERT INTO src VALUES (2)")
        .await?;
    fixture.execute_command("REFRESH DYNAMIC TABLE dt").await?;
    // QueryContext caches table snapshots; use a fresh request context after refresh.
    let read_ctx = fixture.new_query_ctx().await?;
    assert_numbers(
        &read_ctx,
        "SELECT * FROM legacy_dynamic_cte.dt ORDER BY n",
        &[&[1], &[2]],
    )
    .await?;
    Ok(())
}
