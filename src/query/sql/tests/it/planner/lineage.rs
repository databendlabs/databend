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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::CatalogType;
use databend_common_sql::LineageSource;
use databend_common_sql::LineageTarget;
use databend_common_sql::Planner;
use databend_common_sql::QueryExecutor;
use databend_common_sql::QueryLineage;
use databend_common_sql::QueryLineageColumn;
use databend_common_sql::QueryLineageColumnEdge;
use databend_common_sql::QueryLineageKind;
use databend_common_sql::QueryLineageRelation;
use databend_common_sql::QueryLineageRelationKind;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;
use crate::framework::init_testing_globals_with_config;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_insert_select_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    // INSERT INTO dst SELECT a + b AS x FROM src WHERE c > 0
    let sql = "INSERT INTO dst SELECT a + b AS x FROM src WHERE c > 0";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;
    let expected =
        expected_table_query_lineage(QueryLineageKind::Dml, &ctx, "dst", "src", "x", &["a", "b"])
            .await?;

    assert_eq!(lineage, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_stream_passes_through_to_source_table() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT)").await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;
    ctx.register_lineage_stream("default", "src_stream", "src")
        .await?;

    let lineage =
        query_lineage_from_sql(&ctx, "INSERT INTO dst SELECT a + 1 FROM src_stream").await?;
    let expected =
        expected_table_query_lineage(QueryLineageKind::Dml, &ctx, "dst", "src", "x", &["a"])
            .await?;

    assert_eq!(lineage, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_aggregate_arguments_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT)").await?;
    ctx.register_setup_sql("CREATE TABLE dst(all_count UInt64, sum_a Int64)")
        .await?;

    let lineage =
        query_lineage_from_sql(&ctx, "INSERT INTO dst SELECT count(*), sum(a) FROM src").await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "all_count", &[]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "sum_a", &["src.a"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_scalar_subquery_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE left_src(a INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE right_src(b INT, filter_flag INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    let lineage = query_lineage_from_sql(
        &ctx,
        "INSERT INTO dst SELECT a + (SELECT max(b) FROM right_src WHERE filter_flag > 0) FROM left_src",
    )
    .await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "left_src.a",
        "right_src.b",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_insert_lineage_does_not_pin_target_table_info() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT)").await?;
    ctx.register_setup_sql("CREATE TABLE dst(a INT)").await?;

    let plan = ctx.bind_sql("INSERT INTO dst SELECT a FROM src").await?;
    let Plan::Insert(plan) = plan else {
        return Err(ErrorCode::Internal("expected insert plan"));
    };
    // Lineage capture needs the target table id, but ordinary INSERT must keep
    // resolving its target by name at execution time. Populating `table_info`
    // would pin execution to the table object seen during binding, so lineage
    // stores the id in its dedicated field instead.
    assert!(plan.table_info.is_none());
    assert!(plan.lineage_target_table_id.is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_insert_select_with_cte_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    // INSERT INTO dst WITH q AS (SELECT a + b AS x, c FROM src) SELECT x FROM q WHERE c > 0
    let sql =
        "INSERT INTO dst WITH q AS (SELECT a + b AS x, c FROM src) SELECT x FROM q WHERE c > 0";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;
    let expected =
        expected_table_query_lineage(QueryLineageKind::Dml, &ctx, "dst", "src", "x", &["a", "b"])
            .await?;

    assert_eq!(lineage, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_insert_select_with_auto_materialized_cte_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.get_settings().set_enable_auto_materialize_cte(1)?;
    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    // INSERT INTO dst WITH q AS (SELECT a, b, c FROM src) SELECT q1.a + q2.b FROM q q1 JOIN q q2 ON q1.c = q2.c
    let sql = "INSERT INTO dst WITH q AS (SELECT a, b, c FROM src) SELECT q1.a + q2.b FROM q q1 JOIN q q2 ON q1.c = q2.c";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "src.a", "src.b",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_insert_select_with_explicit_materialized_cte_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    let sql = "INSERT INTO dst WITH q(x, filter_col) AS MATERIALIZED (SELECT a + b, c FROM src) SELECT x FROM q WHERE filter_col > 0";
    let mut planner = Planner::new_with_query_executor(
        ctx.clone(),
        Arc::new(LineageQueryExecutor { ctx: ctx.clone() }),
    );
    let (plan, _) = planner.plan_sql(sql).await?;
    let lineage = plan
        .query_lineage()?
        .ok_or_else(|| ErrorCode::Internal(format!("missing query lineage for SQL: {sql}")))?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "src.a", "src.b",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_explicit_materialized_cte_tuple_field_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(t TUPLE(a INT, b INT))")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    let direct = query_lineage_from_sql(&ctx, "INSERT INTO dst SELECT t.1 FROM src").await?;
    assert_lineage_sources(&direct, QueryLineageKind::Dml, "dst", "x", &["src.t:a"]);

    let sql = "INSERT INTO dst WITH q(t) AS MATERIALIZED (SELECT t FROM src) SELECT t.1 FROM q";
    let mut planner = Planner::new_with_query_executor(
        ctx.clone(),
        Arc::new(LineageQueryExecutor { ctx: ctx.clone() }),
    );
    let (plan, _) = planner.plan_sql(sql).await?;
    let lineage = plan
        .query_lineage()?
        .ok_or_else(|| ErrorCode::Internal(format!("missing query lineage for SQL: {sql}")))?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &["src.t:a"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_join_and_exists_filter_columns_are_excluded_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE left_src(id INT, k INT, a INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE right_src(id INT, b INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE filter_src(k INT, flag INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    // INSERT INTO dst SELECT l.a + r.b FROM left_src l JOIN right_src r ON l.id = r.id
    // WHERE EXISTS (SELECT 1 FROM filter_src f WHERE f.k = l.k AND f.flag > 0)
    let sql = "INSERT INTO dst SELECT l.a + r.b FROM left_src l JOIN right_src r ON l.id = r.id WHERE EXISTS (SELECT 1 FROM filter_src f WHERE f.k = l.k AND f.flag > 0)";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "left_src.a",
        "right_src.b",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_join_using_prefers_left_column_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE left_src(id INT, a INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE right_src(id INT, b INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;

    // INSERT INTO dst SELECT id FROM left_src JOIN right_src USING(id)
    let sql = "INSERT INTO dst SELECT id FROM left_src JOIN right_src USING(id)";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "left_src.id",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_create_view_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT, c INT)")
        .await?;

    // CREATE VIEW v(vx) AS SELECT a + b FROM src WHERE c > 0
    let sql = "CREATE VIEW v(vx) AS SELECT a + b FROM src WHERE c > 0";
    let mut plan = ctx.bind_sql(sql).await?;
    let query_plan = ctx.bind_sql("SELECT a + b FROM src WHERE c > 0").await?;
    let Plan::CreateView(create_view) = &mut plan else {
        return Err(ErrorCode::Internal("expected create view plan"));
    };
    create_view.query_plan = Some(Box::new(query_plan));
    let lineage = plan
        .query_lineage()?
        .ok_or_else(|| ErrorCode::Internal("missing create view lineage"))?;
    let expected = expected_query_lineage(
        QueryLineageKind::CreateView,
        relation("v", QueryLineageRelationKind::View, None),
        table_relation(&ctx, "src").await?,
        column("vx", 0),
        vec![
            table_column(&ctx, "src", "a").await?,
            table_column(&ctx, "src", "b").await?,
        ],
    );

    assert_eq!(lineage, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_view_plan_omits_lineage_query_by_default() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    let plan = ctx
        .bind_sql("CREATE VIEW v AS SELECT * FROM missing_table")
        .await?;

    let Plan::CreateView(plan) = plan else {
        return Err(ErrorCode::Internal("expected create view plan"));
    };
    assert!(plan.query_plan.is_none());
    Ok(())
}

#[test]
fn test_query_lineage_insert_select_from_view_stops_at_view_from_sql() -> Result<()> {
    std::thread::Builder::new()
        .name("lineage_view_boundary_sql".to_string())
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|err| ErrorCode::Internal(err.to_string()))?
                .block_on(async {
                    let ctx = lineage_test_context().await?;
                    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT)")
                        .await?;
                    ctx.register_setup_sql("CREATE TABLE dst(x INT)").await?;
                    ctx.register_view_sql("default", "v", "SELECT a + b AS vx FROM src")
                        .await?;

                    // Keep an expression above the view output and rename the view column
                    // in the query scope. Lineage should preserve the view's real output
                    // column name, not the local table-alias column name.
                    let lineage = query_lineage_from_sql(
                        &ctx,
                        "INSERT INTO dst SELECT c + 1 FROM v AS aliased(c)",
                    )
                    .await?;
                    let expected = expected_query_lineage(
                        QueryLineageKind::Dml,
                        table_relation(&ctx, "dst").await?,
                        view_relation(&ctx, "v").await?,
                        table_column(&ctx, "dst", "x").await?,
                        vec![column("vx", 0)],
                    );

                    assert_eq!(lineage, expected);
                    Ok(())
                })
        })
        .map_err(|err| ErrorCode::Internal(err.to_string()))?
        .join()
        .map_err(|_| ErrorCode::Internal("lineage view boundary test panicked"))?
}

#[test]
fn test_query_lineage_ctas_from_view_stops_at_view_from_sql() -> Result<()> {
    std::thread::Builder::new()
        .name("lineage_ctas_view_boundary_sql".to_string())
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|err| ErrorCode::Internal(err.to_string()))?
                .block_on(async {
                    let ctx = lineage_test_context().await?;
                    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT)")
                        .await?;
                    ctx.register_view_sql("default", "v", "SELECT a + b AS vx FROM src")
                        .await?;

                    let lineage = query_lineage_from_sql(
                        &ctx,
                        "CREATE TABLE dst ENGINE=NULL AS SELECT vx AS x FROM v",
                    )
                    .await?;
                    let expected = expected_query_lineage(
                        QueryLineageKind::Ctas,
                        relation("dst", QueryLineageRelationKind::Table, None),
                        view_relation(&ctx, "v").await?,
                        column("x", 0),
                        vec![column("vx", 0)],
                    );

                    assert_eq!(lineage, expected);
                    Ok(())
                })
        })
        .map_err(|err| ErrorCode::Internal(err.to_string()))?
        .join()
        .map_err(|_| ErrorCode::Internal("CTAS view boundary test panicked"))?
}

#[test]
fn test_query_lineage_duplicate_view_outputs_stay_distinct_from_sql() -> Result<()> {
    std::thread::Builder::new()
        .name("lineage_duplicate_view_outputs_sql".to_string())
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|err| ErrorCode::Internal(err.to_string()))?
                .block_on(async {
                    let ctx = lineage_test_context().await?;
                    ctx.register_setup_sql("CREATE TABLE src(a INT)").await?;
                    ctx.register_setup_sql("CREATE TABLE dst(x INT, y INT)")
                        .await?;
                    ctx.register_view_sql("default", "v", "SELECT a AS v1, a AS v2 FROM src")
                        .await?;

                    let lineage =
                        query_lineage_from_sql(&ctx, "INSERT INTO dst SELECT v1, v2 FROM v")
                            .await?;

                    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &["v.v1"]);
                    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "y", &["v.v2"]);
                    Ok(())
                })
        })
        .map_err(|err| ErrorCode::Internal(err.to_string()))?
        .join()
        .map_err(|_| ErrorCode::Internal("duplicate view outputs test panicked"))?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_replace_into_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(id INT, a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(id INT, x INT)")
        .await?;

    // REPLACE INTO dst(id, x) ON(id) SELECT id, a + b FROM src WHERE c > 0
    let sql = "REPLACE INTO dst(id, x) ON(id) SELECT id, a + b FROM src WHERE c > 0";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "id", &["src.id"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "src.a", "src.b",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_multi_insert_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst1(x INT, y INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst2(x INT, y INT)")
        .await?;

    // INSERT ALL INTO dst1 VALUES(a, b) INTO dst2(y) VALUES(c) SELECT a, b, c FROM src
    // The projected dst2(y) target list must still keep dst2's stable table id and y's real
    // table column id instead of using the insert-list ordinal 0.
    let sql = "INSERT ALL INTO dst1 VALUES(a, b) INTO dst2(y) VALUES(c) SELECT a, b, c FROM src";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst1", "x", &["src.a"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst1", "y", &["src.b"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst2", "y", &["src.c"]);
    assert_target_relation_id(&lineage, "dst2", table_id(&ctx, "dst2").await?);
    assert_target_column_id(&lineage, "dst2", "y", column_id(&ctx, "dst2", "y").await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_update_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(id INT, a INT, b INT, c INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(id INT, x INT)")
        .await?;

    // UPDATE dst SET x = src.a + src.b FROM src WHERE dst.id = src.id AND src.c > 0
    let sql = "UPDATE dst SET x = src.a + src.b FROM src WHERE dst.id = src.id AND src.c > 0";
    let lineage = query_lineage_from_bound_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &[
        "src.a", "src.b",
    ]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_merge_multiple_when_from_sql() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(id INT, a INT, b INT, c INT, d INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(id INT, x INT, y INT)")
        .await?;

    // MERGE INTO dst USING src ON dst.id = src.id
    // WHEN MATCHED AND src.c > 0 THEN UPDATE SET x = src.a
    // WHEN MATCHED AND src.d > 0 THEN UPDATE SET y = src.b
    // WHEN NOT MATCHED THEN INSERT (id, x, y) VALUES (src.id, src.a, src.b)
    let sql = "MERGE INTO dst USING src ON dst.id = src.id WHEN MATCHED AND src.c > 0 THEN UPDATE SET x = src.a WHEN MATCHED AND src.d > 0 THEN UPDATE SET y = src.b WHEN NOT MATCHED THEN INSERT (id, x, y) VALUES (src.id, src.a, src.b)";
    let lineage = query_lineage_from_bound_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "id", &["src.id"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "x", &["src.a"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "y", &["src.b"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_lineage_merge_insert_subset_preserves_target_column_id() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(id INT, b INT)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE dst(id INT, x INT, y INT)")
        .await?;

    // MERGE unmatched INSERT can project a subset of target columns. The target column id must
    // come from dst.y's table schema, not from its position in the INSERT list.
    let sql = "MERGE INTO dst USING src ON dst.id = src.id WHEN NOT MATCHED THEN INSERT (y) VALUES (src.b)";
    let lineage = query_lineage_from_bound_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst", "y", &["src.b"]);
    assert_target_column_id(&lineage, "dst", "y", column_id(&ctx, "dst", "y").await?);
    Ok(())
}

async fn lineage_test_context() -> Result<Arc<LiteTableContext>> {
    let mut config = InnerConfig::default();
    config.lineage.lineage_on = true;
    // Lite globals are thread-local in debug builds. Initialize capture on the test's current
    // thread before LiteTableContext::create() attempts to install the default configuration.
    init_testing_globals_with_config(config);
    LiteTableContext::create().await
}

struct LineageQueryExecutor {
    ctx: Arc<LiteTableContext>,
}

#[async_trait::async_trait]
impl QueryExecutor for LineageQueryExecutor {
    async fn execute_query_with_sql_string(&self, sql: &str) -> Result<Vec<DataBlock>> {
        self.ctx.register_table_sql(sql).await?;
        Ok(Vec::new())
    }
}

async fn query_lineage_from_sql(ctx: &Arc<LiteTableContext>, sql: &str) -> Result<QueryLineage> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    plan.query_lineage()?
        .ok_or_else(|| ErrorCode::Internal(format!("missing query lineage for SQL: {sql}")))
}

async fn query_lineage_from_bound_sql(
    ctx: &Arc<LiteTableContext>,
    sql: &str,
) -> Result<QueryLineage> {
    let plan = ctx.bind_sql(sql).await?;
    plan.query_lineage()?
        .ok_or_else(|| ErrorCode::Internal(format!("missing query lineage for SQL: {sql}")))
}

fn assert_lineage_sources(
    lineage: &QueryLineage,
    kind: QueryLineageKind,
    target_table: &str,
    target_column: &str,
    expected: &[&str],
) {
    assert_eq!(lineage.kind, kind, "unexpected lineage kind: {lineage:?}");

    let mut actual = lineage
        .targets
        .iter()
        .find(|target| target.relation.name == target_table)
        .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"))
        .sources
        .iter()
        .flat_map(|from_relation| {
            from_relation
                .columns
                .iter()
                .filter(|edge| edge.target.name == target_column)
                .map(|edge| format!("{}.{}", from_relation.relation.name, edge.source.name))
        })
        .collect::<Vec<_>>();
    actual.sort();
    actual.dedup();

    let mut expected = expected
        .iter()
        .map(|source| source.to_string())
        .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(
        actual, expected,
        "unexpected lineage for {target_table}.{target_column}: {lineage:?}"
    );
}

fn assert_target_relation_id(lineage: &QueryLineage, target_table: &str, expected_id: u64) {
    let target = lineage
        .targets
        .iter()
        .find(|target| target.relation.name == target_table)
        .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"));
    assert_eq!(
        target.relation.id,
        Some(expected_id),
        "unexpected target relation id for {target_table}: {lineage:?}"
    );
}

fn assert_target_column_id(
    lineage: &QueryLineage,
    target_table: &str,
    target_column: &str,
    expected_id: ColumnId,
) {
    let actual = lineage
        .targets
        .iter()
        .find(|target| target.relation.name == target_table)
        .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"))
        .sources
        .iter()
        .flat_map(|source| source.columns.iter())
        .find(|edge| edge.target.name == target_column)
        .map(|edge| edge.target.id);
    assert_eq!(
        actual,
        Some(expected_id),
        "unexpected target column id for {target_table}.{target_column}: {lineage:?}"
    );
}

async fn table_id(ctx: &Arc<LiteTableContext>, table: &str) -> Result<u64> {
    Ok(ctx
        .get_table("default", "default", table)
        .await?
        .get_table_info()
        .ident
        .table_id)
}

async fn column_id(ctx: &Arc<LiteTableContext>, table: &str, column: &str) -> Result<ColumnId> {
    ctx.get_table("default", "default", table)
        .await?
        .schema()
        .column_id_of(column)
}

async fn expected_table_query_lineage(
    kind: QueryLineageKind,
    ctx: &Arc<LiteTableContext>,
    to_table: &str,
    from_table: &str,
    to_column: &str,
    from_columns: &[&str],
) -> Result<QueryLineage> {
    let mut sources = Vec::with_capacity(from_columns.len());
    for from_column in from_columns {
        sources.push(table_column(ctx, from_table, from_column).await?);
    }

    Ok(expected_query_lineage(
        kind,
        table_relation(ctx, to_table).await?,
        table_relation(ctx, from_table).await?,
        table_column(ctx, to_table, to_column).await?,
        sources,
    ))
}

async fn table_relation(ctx: &Arc<LiteTableContext>, table: &str) -> Result<QueryLineageRelation> {
    Ok(relation(
        table,
        QueryLineageRelationKind::Table,
        Some(table_id(ctx, table).await?),
    ))
}

async fn view_relation(ctx: &Arc<LiteTableContext>, view: &str) -> Result<QueryLineageRelation> {
    Ok(relation(
        view,
        QueryLineageRelationKind::View,
        Some(table_id(ctx, view).await?),
    ))
}

async fn table_column(
    ctx: &Arc<LiteTableContext>,
    table: &str,
    column_name: &str,
) -> Result<QueryLineageColumn> {
    Ok(column(
        column_name,
        column_id(ctx, table, column_name).await?,
    ))
}

fn expected_query_lineage(
    kind: QueryLineageKind,
    target: QueryLineageRelation,
    source: QueryLineageRelation,
    target_column: QueryLineageColumn,
    source_columns: Vec<QueryLineageColumn>,
) -> QueryLineage {
    QueryLineage {
        kind,
        targets: vec![LineageTarget {
            relation: target,
            sources: vec![LineageSource {
                relation: source,
                columns: source_columns
                    .into_iter()
                    .map(|source| QueryLineageColumnEdge {
                        source,
                        target: target_column.clone(),
                    })
                    .collect(),
            }],
        }],
    }
}

fn relation(name: &str, kind: QueryLineageRelationKind, id: Option<u64>) -> QueryLineageRelation {
    QueryLineageRelation {
        catalog: "default".to_string(),
        database: "default".to_string(),
        name: name.to_string(),
        id,
        catalog_type: Some(CatalogType::Default),
        kind,
    }
}

fn column(name: &str, id: ColumnId) -> QueryLineageColumn {
    QueryLineageColumn {
        name: name.to_string(),
        id,
    }
}
