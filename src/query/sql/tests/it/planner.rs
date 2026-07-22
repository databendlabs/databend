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

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_catalog::table_context::TableContextVariables;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_sql::FormatOptions;
use databend_common_sql::LineageDownstream;
use databend_common_sql::LineageUpstream;
use databend_common_sql::MetadataRef;
use databend_common_sql::Planner;
use databend_common_sql::QueryLineage;
use databend_common_sql::QueryLineageColumn;
use databend_common_sql::QueryLineageColumnEdge;
use databend_common_sql::QueryLineageKind;
use databend_common_sql::QueryLineageRelation;
use databend_common_sql::QueryLineageRelationKind;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql_test_support::TestCase;
use databend_common_sql_test_support::TestCaseRunner;
use databend_common_sql_test_support::TestSuite;
use databend_common_sql_test_support::TestSuiteMints;
use databend_common_sql_test_support::init_testing_globals_with_config;
use databend_common_sql_test_support::run_test_case_core;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct LiteRunner(Arc<LiteTableContext>);

struct LiteReplayCaseSpec {
    name: &'static str,
    warehouse_distribution: bool,
    optimizer_skip_list: &'static [&'static str],
    default_node_num: u64,
}

impl LiteReplayCaseSpec {
    fn matches(&self, case: &TestCase) -> bool {
        case.stem == self.name || case.name == self.name
    }

    fn configure(&self, ctx: &Arc<LiteTableContext>, case: &TestCase) -> Result<()> {
        ctx.configure_for_optimizer_case(case.auto_stats)?;
        ctx.set_table_warehouse_distribution(self.warehouse_distribution);

        if !self.optimizer_skip_list.is_empty() {
            ctx.get_settings()
                .set_optimizer_skip_list(self.optimizer_skip_list.join(","))?;
        }

        ctx.set_cluster_node_num(case.node_num.unwrap_or(self.default_node_num));
        Ok(())
    }
}

const LITE_REPLAY_CASE_SPECS: &[LiteReplayCaseSpec] = &[
    LiteReplayCaseSpec {
        name: "01_cross_join_aggregation",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "01_multi_join_avg_case_expression",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "01_multi_join_sum_case_expression",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "Q01",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "Q03",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 2,
    },
    LiteReplayCaseSpec {
        name: "eager_q0",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "eager_q1",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "eager_q2",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "eager_q3",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "19574_correlated_exists_union",
        warehouse_distribution: false,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "19574_correlated_exists_union_all",
        warehouse_distribution: false,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "q17_histogram_join_order",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
    LiteReplayCaseSpec {
        name: "q10_scaled_join_ndv",
        warehouse_distribution: true,
        optimizer_skip_list: &[],
        default_node_num: 1,
    },
];

impl TestCaseRunner for LiteRunner {
    async fn bind_sql(&self, sql: &str) -> Result<databend_common_sql::plans::Plan> {
        self.0.bind_sql(sql).await
    }

    async fn optimize_plan(
        &self,
        plan: databend_common_sql::plans::Plan,
    ) -> Result<databend_common_sql::plans::Plan> {
        self.0.optimize_plan(plan).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lite_replay_service_optimizer_cases() -> Result<()> {
    let suite = TestSuite::new(
        TestSuite::optimizer_data_dir(),
        std::env::var("TEST_SUBDIR").ok(),
    );
    let mut mints = suite.create_mints();

    for (case, spec) in suite.load_cases()?.into_iter().filter_map(|case| {
        LITE_REPLAY_CASE_SPECS
            .iter()
            .find(|spec| spec.matches(&case))
            .map(|spec| (case, spec))
    }) {
        let ctx = LiteTableContext::create().await?;
        run_test_case(&ctx, &case, spec, &mut mints).await?;
    }
    Ok(())
}

struct StatisticsTraceGoldenCase {
    name: &'static str,
    description: &'static str,
    trace_file: &'static str,
    sql_file: &'static str,
}

fn read_statistics_trace_fixture(case: &StatisticsTraceGoldenCase, kind: &str) -> Result<String> {
    let path = Path::new(&TestSuite::optimizer_data_dir())
        .join("statistics_trace")
        .join(kind)
        .join(match kind {
            "sql" => case.sql_file,
            "traces" => case.trace_file,
            _ => unreachable!("unknown statistics trace fixture kind"),
        });
    std::fs::read_to_string(&path).map_err(|err| {
        ErrorCode::Internal(format!(
            "failed to read statistics trace fixture {}: {err}",
            path.display()
        ))
    })
}

async fn write_statistics_trace_case(
    file: &mut impl Write,
    case: &StatisticsTraceGoldenCase,
) -> Result<()> {
    let (sql, optimized_plan) = replay_statistics_trace_case(case).await?;
    let optimized = optimized_plan.format_indent(FormatOptions::default())?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "trace: {}", case.trace_file)?;
    writeln!(file, "sql_file: {}", case.sql_file)?;
    writeln!(file, "sql:")?;
    writeln!(file, "{}", sql.trim())?;
    writeln!(file, "optimized_plan:")?;
    writeln!(file, "{optimized}")?;
    writeln!(file)?;
    Ok(())
}

async fn write_statistics_trace_summary_case(
    file: &mut impl Write,
    case: &StatisticsTraceGoldenCase,
) -> Result<()> {
    let (sql, optimized_plan) = replay_statistics_trace_case(case).await?;
    let summary = format_statistics_trace_summary(&optimized_plan)?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "trace: {}", case.trace_file)?;
    writeln!(file, "sql_file: {}", case.sql_file)?;
    writeln!(file, "sql:")?;
    writeln!(file, "{}", sql.trim())?;
    writeln!(file, "replay_summary:")?;
    writeln!(file, "{summary}")?;
    writeln!(file)?;
    Ok(())
}

async fn replay_statistics_trace_case(case: &StatisticsTraceGoldenCase) -> Result<(String, Plan)> {
    let sql = read_statistics_trace_fixture(case, "sql")?;
    let trace_input = read_statistics_trace_fixture(case, "traces")?;
    let input = serde_json::from_str(&trace_input).map_err(|err| {
        ErrorCode::Internal(format!(
            "invalid statistics trace fixture {}: {err}",
            case.trace_file
        ))
    })?;
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;
    ctx.register_replay_input(&input).await?;

    let raw_plan = ctx.bind_sql(&sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;
    Ok((sql, optimized_plan))
}

fn format_statistics_trace_summary(plan: &Plan) -> Result<String> {
    let Plan::Query {
        s_expr, metadata, ..
    } = plan
    else {
        return Err(ErrorCode::Internal(
            "statistics trace replay summary expects query plan",
        ));
    };

    Ok(statistics_trace_summary_tree(s_expr, metadata)?.format_pretty()?)
}

fn statistics_trace_summary_tree(s_expr: &SExpr, metadata: &MetadataRef) -> Result<FormatTreeNode> {
    match s_expr.plan() {
        RelOperator::MaterializedCTE(cte) => {
            let children = s_expr
                .children()
                .map(|child| statistics_trace_summary_tree(child, metadata))
                .collect::<Result<Vec<_>>>()?;
            Ok(FormatTreeNode::with_children(
                format!("MaterializedCTE: {} refs={}", cte.cte_name, cte.ref_count),
                children,
            ))
        }
        RelOperator::MaterializedCTERef(cte_ref) => Ok(FormatTreeNode::new(format!(
            "MaterializedCTERef: {} output_columns={}",
            cte_ref.cte_name,
            cte_ref.output_columns.len()
        ))),
        RelOperator::Scan(scan) => {
            let metadata = metadata.read();
            let table = metadata.table(scan.table_index);
            let rows = scan
                .statistics
                .table_stats
                .as_ref()
                .and_then(|stats| stats.num_rows)
                .map_or_else(|| "None".to_string(), |rows| rows.to_string());
            let row_access_policy = if scan.secure_predicates.is_some() {
                " row_access_policy=true"
            } else {
                ""
            };
            Ok(FormatTreeNode::new(format!(
                "Scan: {}.{} (#{}) rows={}{}",
                table.database(),
                table.name(),
                scan.table_index,
                rows,
                row_access_policy
            )))
        }
        RelOperator::Join(join) => {
            let children = s_expr
                .children()
                .map(|child| statistics_trace_summary_tree(child, metadata))
                .collect::<Result<Vec<_>>>()?;
            Ok(FormatTreeNode::with_children(
                format!("Join: {}", join.join_type),
                children,
            ))
        }
        RelOperator::Udf(udf) => {
            let children = s_expr
                .children()
                .map(|child| statistics_trace_summary_tree(child, metadata))
                .collect::<Result<Vec<_>>>()?;
            let name = if udf.script_udf { "UdfScript" } else { "Udf" };
            Ok(FormatTreeNode::with_children(name.to_string(), children))
        }
        _ => {
            let children = s_expr
                .children()
                .map(|child| statistics_trace_summary_tree(child, metadata))
                .collect::<Result<Vec<_>>>()?;
            Ok(FormatTreeNode::with_children(
                format!("{:?}", s_expr.plan().rel_op()),
                children,
            ))
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lite_replay_statistics_trace_golden() -> Result<()> {
    let mut file = open_golden_file("planner", "statistics_trace.txt")?;
    let cases = [
        StatisticsTraceGoldenCase {
            name: "empty_self_join",
            description: "Replay the JSON fixture generated by the service-side CollectStatisticsOptimizer trace test.",
            trace_file: "empty_self_join.json",
            sql_file: "empty_self_join.sql",
        },
        StatisticsTraceGoldenCase {
            name: "tpch_returned_orders",
            description: "Rebuild a mock catalog from StatisticsTrace JSON for a CTE, aggregation, filtered three-way join, sort, and limit.",
            trace_file: "tpch_returned_orders.json",
            sql_file: "tpch_returned_orders.sql",
        },
        StatisticsTraceGoldenCase {
            name: "customer_self_join",
            description: "Use two trace table indexes that map to the same table name to replay a self join without table DDL.",
            trace_file: "customer_self_join.json",
            sql_file: "customer_self_join.sql",
        },
    ];

    for case in &cases {
        write_statistics_trace_case(&mut file, case).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lite_replay_statistics_trace_materialized_cte_golden() -> Result<()> {
    let mut file = open_golden_file("planner", "statistics_trace_materialized_cte.txt")?;
    let case = StatisticsTraceGoldenCase {
        name: "view_materialized_cte_join",
        description: "Replay the service-collected trace with view, UDF, row access policy, non-empty stats, and auto-materialized CTE.",
        trace_file: "view_materialized_cte_join.json",
        sql_file: "view_materialized_cte_join.sql",
    };

    write_statistics_trace_summary_case(&mut file, &case).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_like_escape_preserves_existing_binding_semantics() -> Result<()> {
    let ctx = LiteTableContext::create().await?;

    for sql in [
        "SELECT 'a' LIKE 'a' ESCAPE ''",
        "SELECT '%++' NOT LIKE '*%++' ESCAPE '*'",
        "SELECT 'a' LIKE concat('a') ESCAPE ''",
        "SELECT '%' LIKE '\\\\%' ESCAPE ''",
        "SELECT like_any('%', '\\\\%', '')",
        "SELECT 'a' LIKE ANY ('a', 'b') ESCAPE ''",
        "SELECT 'a' LIKE ANY (SELECT 'a') ESCAPE ''",
    ] {
        ctx.bind_sql(sql).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_subquery_project_set_keeps_lambda_udf_argument_columns() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_setup_sql(
        "CREATE FUNCTION ddb_string_split_compat AS (s, delim) -> CASE
            WHEN s IS NULL THEN NULL
            WHEN delim IS NULL THEN [s]
            WHEN delim = '' THEN REGEXP_SPLIT_TO_ARRAY(s, '')
            ELSE SPLIT(s, delim)
        END",
    )
    .await?;
    ctx.register_setup_sql("CREATE TABLE documents(id INT, s VARCHAR)")
        .await?;

    let plan = ctx
        .bind_sql(
            "SELECT ss FROM (
                SELECT id, UNNEST(ddb_string_split_compat(s, 'bb')) AS ss
                FROM documents WHERE 1
            ) AS q ORDER BY id",
        )
        .await?;
    let plan = ctx.optimize_plan(plan).await?;
    let plan = plan.format_indent(Default::default())?;
    assert!(
        plan.contains("split(documents.s"),
        "ProjectSet should keep the lambda UDF body bound to documents.s:\n{plan}"
    );
    assert!(
        !plan.contains("split('bb'"),
        "UDF parameter replacement should not overwrite outer column references:\n{plan}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_execute_immediate_binds_session_variable_script() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_variable(
        "exec_script".to_string(),
        Scalar::String("select 42".to_string()),
    );

    ctx.bind_sql("EXECUTE IMMEDIATE $exec_script").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_time_travel_binds_session_variable_snapshot() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_setup_sql("CREATE TABLE t(c int)").await?;
    ctx.set_variable(
        "first_snap".to_string(),
        Scalar::String("snapshot-id".to_string()),
    );

    let err = ctx
        .bind_sql("SELECT * FROM t AT(SNAPSHOT => $first_snap)")
        .await
        .unwrap_err();
    assert!(
        err.message()
            .contains("Time travel operation is not supported"),
        "unexpected error: {err:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_rewrite_boundaries_preserve_legacy_function_forms() -> Result<()> {
    let ctx = LiteTableContext::create().await?;

    ctx.bind_sql("SELECT IFNULL(1, 2), IFNULL(NULL), NVL(NULL)")
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_large_inlist_threshold_binds_constant_values() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_setup_sql("CREATE TABLE t1(a int, b int)")
        .await?;

    let values = (0..=1300)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT * FROM t1 WHERE a NOT IN ({values})");
    ctx.bind_sql(&sql).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_srf_rejects_window_argument_before_project_set_binding() -> Result<()> {
    let ctx = LiteTableContext::create().await?;

    let err = ctx
        .bind_sql("SELECT unnest(first_value('aa') OVER (PARTITION BY 'bb'))")
        .await
        .unwrap_err();
    assert_eq!(err.code(), 1065, "unexpected error: {err:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lambda_udf_resolves_own_parameters() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_setup_sql("CREATE FUNCTION f1 AS (p) -> (p)")
        .await?;
    ctx.register_setup_sql("CREATE FUNCTION f2 AS (p) -> (p)")
        .await?;
    ctx.register_setup_sql("CREATE TABLE t(i UInt8 NOT NULL)")
        .await?;

    ctx.bind_sql("SELECT f1(1)").await?;
    for sql in [
        "SELECT 1 FROM (SELECT f2(f1(10)))",
        "SELECT * FROM t WHERE f2(f1(1))",
        "SELECT i, nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t",
        "SELECT CASE WHEN i > f2(f1(100)) THEN 200 ELSE 100 END FROM t",
    ] {
        let plan = ctx.bind_sql(sql).await?;
        ctx.optimize_plan(plan).await?;
    }
    ctx.bind_sql("INSERT INTO t VALUES (f2(f1(1)))").await?;
    ctx.bind_sql("UPDATE t SET i=f2(f1(2)) WHERE i=f2(f1(1))")
        .await?;
    Ok(())
}

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
async fn test_insert_lineage_does_not_pin_target_table_info() -> Result<()> {
    let ctx = lineage_test_context().await?;
    ctx.register_setup_sql("CREATE TABLE src(a INT)").await?;
    ctx.register_setup_sql("CREATE TABLE dst(a INT)").await?;

    let plan = ctx.bind_sql("INSERT INTO dst SELECT a FROM src").await?;
    let Plan::Insert(plan) = plan else {
        return Err(ErrorCode::Internal("expected insert plan"));
    };
    assert!(plan.table_info.is_none());
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
    let lineage = query_lineage_from_sql(&ctx, sql).await?;
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
async fn test_create_view_does_not_bind_query_when_lineage_capture_is_disabled() -> Result<()> {
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

                    // INSERT INTO dst SELECT vx FROM v
                    let lineage =
                        query_lineage_from_sql(&ctx, "INSERT INTO dst SELECT vx FROM v").await?;
                    let expected = expected_query_lineage(
                        QueryLineageKind::Dml,
                        table_relation(&ctx, "dst").await?,
                        view_relation(&ctx, "v").await?,
                        table_column(&ctx, "dst", "x").await?,
                        vec![table_column(&ctx, "v", "vx").await?],
                    );

                    assert_eq!(lineage, expected);
                    Ok(())
                })
        })
        .map_err(|err| ErrorCode::Internal(err.to_string()))?
        .join()
        .map_err(|_| ErrorCode::Internal("lineage view boundary test panicked"))?
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

    // INSERT ALL INTO dst1 VALUES(a, b) INTO dst2(x, y) VALUES(b, c) SELECT a, b, c FROM src
    let sql =
        "INSERT ALL INTO dst1 VALUES(a, b) INTO dst2(x, y) VALUES(b, c) SELECT a, b, c FROM src";
    let lineage = query_lineage_from_sql(&ctx, sql).await?;

    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst1", "x", &["src.a"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst1", "y", &["src.b"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst2", "x", &["src.b"]);
    assert_lineage_sources(&lineage, QueryLineageKind::Dml, "dst2", "y", &["src.c"]);
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

async fn lineage_test_context() -> Result<Arc<LiteTableContext>> {
    let mut config = InnerConfig::default();
    config.query.common.lineage.capture_enabled = true;
    init_testing_globals_with_config(config);
    LiteTableContext::create().await
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
        .downstreams
        .iter()
        .find(|downstream| downstream.relation.name == target_table)
        .unwrap_or_else(|| panic!("missing target table {target_table}: {lineage:?}"))
        .upstreams
        .iter()
        .flat_map(|from_relation| {
            from_relation
                .columns
                .iter()
                .filter(|edge| edge.downstream.name == target_column)
                .map(|edge| format!("{}.{}", from_relation.relation.name, edge.upstream.name))
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
    downstream: QueryLineageRelation,
    upstream: QueryLineageRelation,
    downstream_column: QueryLineageColumn,
    upstream_columns: Vec<QueryLineageColumn>,
) -> QueryLineage {
    QueryLineage {
        kind,
        downstreams: vec![LineageDownstream {
            relation: downstream,
            upstreams: vec![LineageUpstream {
                relation: upstream,
                columns: upstream_columns
                    .into_iter()
                    .map(|upstream| QueryLineageColumnEdge {
                        upstream,
                        downstream: downstream_column.clone(),
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
        kind,
    }
}

fn column(name: &str, id: ColumnId) -> QueryLineageColumn {
    QueryLineageColumn {
        name: name.to_string(),
        id,
    }
}

async fn setup_tables(ctx: &Arc<LiteTableContext>, case: &TestCase) -> Result<()> {
    for sql in case.tables.values() {
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            ctx.register_setup_sql(statement).await?;
        }
    }
    Ok(())
}

async fn run_test_case(
    ctx: &Arc<LiteTableContext>,
    case: &TestCase,
    spec: &LiteReplayCaseSpec,
    mints: &mut TestSuiteMints,
) -> Result<()> {
    spec.configure(ctx, case)?;
    setup_tables(ctx, case).await?;

    let runner = LiteRunner(ctx.clone());
    run_test_case_core(case, mints.mint_for(case), &runner).await?;
    Ok(())
}

async fn plan_sql(fixture: &Arc<LiteTableContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(fixture.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(plan)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_rest() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='rest', URI='http://127.0.0.1:8080', WAREHOUSE='demo')";
    let plan = plan_sql(&fixture, sql).await?;
    let Plan::CreateCatalog(plan) = plan else {
        panic!("expected CreateCatalog")
    };
    let CatalogOption::Paimon(option) = plan.meta.catalog_option else {
        panic!("expected paimon")
    };
    assert_eq!(option.options["metastore"], "rest");
    assert_eq!(option.options["uri"], "http://127.0.0.1:8080");
    assert_eq!(option.options["warehouse"], "demo");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_filesystem() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='filesystem', WAREHOUSE='s3://bucket/warehouse')";
    let plan = plan_sql(&fixture, sql).await?;
    let Plan::CreateCatalog(plan) = plan else {
        panic!("expected CreateCatalog")
    };
    let CatalogOption::Paimon(option) = plan.meta.catalog_option else {
        panic!("expected paimon")
    };
    assert_eq!(option.options["metastore"], "filesystem");
    assert_eq!(option.options["warehouse"], "s3://bucket/warehouse");
    Ok(())
}

/// Quoted dotted CONNECTION keys keep the quotes in the plan map; catalog try_create strips them.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_filesystem_s3_quoted_keys() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = r#"CREATE CATALOG p TYPE = PAIMON CONNECTION = (
        METASTORE='filesystem',
        WAREHOUSE='s3://bucket/warehouse',
        "s3.endpoint"='http://127.0.0.1:9900',
        "s3.region"='us-east-1'
    )"#;
    let plan = plan_sql(&fixture, sql).await?;
    let Plan::CreateCatalog(plan) = plan else {
        panic!("expected CreateCatalog")
    };
    let CatalogOption::Paimon(option) = plan.meta.catalog_option else {
        panic!("expected paimon")
    };
    // Parser stores quoted idents with quotes intact (Iceberg/Paimon both strip at catalog build).
    assert_eq!(
        option.options.get("\"s3.region\"").map(String::as_str),
        Some("us-east-1")
    );
    assert_eq!(
        option.options.get("\"s3.endpoint\"").map(String::as_str),
        Some("http://127.0.0.1:9900")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_missing_warehouse() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='filesystem')";
    let err = plan_sql(&fixture, sql).await.unwrap_err();
    assert!(
        err.message()
            .contains("warehouse for paimon catalog is not specified")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_rest_missing_uri() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='rest', WAREHOUSE='demo')";
    let err = plan_sql(&fixture, sql).await.unwrap_err();
    assert!(
        err.message()
            .contains("uri for paimon rest catalog is not specified")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn paimon_catalog_invalid_metastore() -> Result<()> {
    let fixture = LiteTableContext::create().await?;
    let sql = "CREATE CATALOG p TYPE = PAIMON CONNECTION = (METASTORE='hive', WAREHOUSE='demo')";
    let err = plan_sql(&fixture, sql).await.unwrap_err();
    assert!(
        err.message()
            .contains("paimon catalog metastore hive is not supported")
    );
    Ok(())
}
