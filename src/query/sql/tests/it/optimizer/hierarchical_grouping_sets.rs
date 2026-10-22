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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::ExprVisitor;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::PVisitAction as VisitAction;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOp;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    enable_cascading: bool,
) -> Result<Plan> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(2);
    ctx.set_table_warehouse_distribution(true);
    for setup_sql in case.setup_sqls {
        ctx.register_setup_sql(setup_sql).await?;
    }
    ctx.get_settings()
        .set_setting("grouping_sets_to_union".to_string(), "1".to_string())?;
    ctx.get_settings().set_setting(
        "enable_cascading_grouping_sets".to_string(),
        u8::from(enable_cascading).to_string(),
    )?;

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;
    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file)?;

    Ok(optimized_plan)
}

fn assert_no_serial_sequence_producer(plan: &Plan) -> Result<()> {
    let Plan::Query { s_expr, .. } = plan else {
        unreachable!("the ROLLUP test query should bind to Plan::Query")
    };

    struct SequenceDistributionChecker;

    impl ExprVisitor<databend_common_sql::optimizer::ir::Physical> for SequenceDistributionChecker {
        fn visit(&mut self, expr: &PExpr) -> Result<VisitAction> {
            if expr.plan().rel_op() == RelOp::Sequence {
                let left_prop = RelExpr::with_p_expr(expr.left_child()).derive_physical_prop()?;
                assert_ne!(
                    left_prop.distribution,
                    Distribution::Serial,
                    "a Serial Sequence producer removes every Exchange from the query"
                );
            }
            Ok(VisitAction::Continue)
        }
    }

    s_expr
        .planned()?
        .expr()
        .accept(&mut SequenceDistributionChecker)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_hierarchical_grouping_sets_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "hierarchical_grouping_sets.txt")?;

    let shared_base = SqlTestCase {
        name: "rollup_shared_base",
        description: "The default hierarchy derives every lower ROLLUP level from the most detailed grouping.",
        setup_sqls: &[ROLLUP_TABLE],
        sql: ROLLUP_QUERY,
    };
    let optimized = write_optimized_case(&mut file, &shared_base, false).await?;
    assert_no_serial_sequence_producer(&optimized)?;

    let cascading = SqlTestCase {
        name: "rollup_nearest_parent_cascade",
        description: "The optional cascade derives every lower ROLLUP level from its closest strict superset.",
        setup_sqls: &[ROLLUP_TABLE],
        sql: ROLLUP_QUERY,
    };
    let optimized = write_optimized_case(&mut file, &cascading, true).await?;
    assert_no_serial_sequence_producer(&optimized)?;

    for case in [
        SqlTestCase {
            name: "rollup_lateral",
            description: "LATERAL decorrelation clones nullable grouping outputs and the generated grouping id.",
            setup_sqls: &["CREATE TABLE t1(a Int32)", "CREATE TABLE t3(n Int32)"],
            sql: "SELECT x.a, q.n FROM (SELECT a FROM t1 GROUP BY ROLLUP(a)) x JOIN LATERAL (SELECT n FROM t3 WHERE n > x.a) q ON true",
        },
        SqlTestCase {
            name: "rollup_scalar_subquery",
            description: "Scalar correlation uses nullable grouping output metadata.",
            setup_sqls: &["CREATE TABLE t1(a Int32)", "CREATE TABLE t3(n Int32)"],
            sql: "SELECT x.a, (SELECT max(n) FROM t3 WHERE n > x.a) FROM (SELECT a FROM t1 GROUP BY ROLLUP(a)) x",
        },
        SqlTestCase {
            name: "outer_rollup_scalar_subquery",
            description: "A SELECT subquery of a grouped SELECT correlates to the nullable key output, not its input.",
            setup_sqls: &[
                "CREATE TABLE t1(a Int32 NOT NULL)",
                "CREATE TABLE t3(n Int32 NOT NULL)",
            ],
            sql: "SELECT a, (SELECT max(n) FROM t3 WHERE n > t1.a) FROM t1 GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "outer_rollup_scalar_sort_key",
            description: "A correlated ORDER BY key in a subquery binds to the key output even when no projection references it.",
            setup_sqls: &["CREATE TABLE t1(a Int32)", "CREATE TABLE t3(n Int32)"],
            sql: "SELECT a, (SELECT n FROM t3 ORDER BY t1.a, n DESC LIMIT 1) FROM t1 GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "outer_rollup_scalar_projection",
            description: "A correlated expression over a key has the nullable key type from the start.",
            setup_sqls: &["CREATE TABLE t1(a Int32)"],
            sql: "SELECT a, (SELECT t1.a + 1) FROM t1 GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "outer_rollup_exists",
            description: "EXISTS must keep using the grouped output, including the NULL total row.",
            setup_sqls: &["CREATE TABLE t1(a Int32)", "CREATE TABLE t3(n Int32)"],
            sql: "SELECT a, EXISTS (SELECT 1 FROM t3 WHERE n > t1.a) FROM t1 GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "outer_rollup_scalar_union",
            description: "UNION inputs and coercions in a subquery see the nullable type of a correlated key.",
            setup_sqls: &["CREATE TABLE t1(a Int32)", "CREATE TABLE t3(n Int32)"],
            sql: "SELECT a, (SELECT max(v) FROM (SELECT n + t1.a AS v FROM t3 UNION ALL SELECT n + 1 AS v FROM t3) s) FROM t1 GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "outer_rollup_scalar_window",
            description: "A correlated window input in a subquery has the nullable key type.",
            setup_sqls: &["CREATE TABLE t1(a Int32)", "CREATE TABLE t3(n Int32)"],
            sql: "SELECT a, (SELECT first_value(n + t1.a) OVER (ORDER BY n) FROM t3 LIMIT 1) FROM t1 GROUP BY ROLLUP(a)",
        },
        SqlTestCase {
            name: "rollup_computed_aggregate_arguments",
            description: "Computed aggregate arguments and ordered aggregate keys must not reuse nullable grouping outputs.",
            setup_sqls: &["CREATE TABLE t1(a Int32)"],
            sql: "SELECT a + 1 AS k, sum(a + 1), min(a + 1), string_agg(to_string(a), ',') WITHIN GROUP (ORDER BY a + 1) FROM t1 GROUP BY ROLLUP(1)",
        },
        SqlTestCase {
            name: "rollup_computed_window_input",
            description: "Window arguments over a computed key bind to the key output.",
            setup_sqls: &["CREATE TABLE t1(a Int32 NOT NULL)"],
            sql: "SELECT a + 1 AS k, grouping(k), sum(a + 1 + 1) OVER () FROM t1 GROUP BY ROLLUP(1)",
        },
        SqlTestCase {
            name: "rollup_window_nullable_signature",
            description: "Repeated LAG expressions over a key share one window function.",
            setup_sqls: &["CREATE TABLE t1(a Int32)"],
            sql: "SELECT a + 1 AS k, lag(k, 1, 0) OVER (ORDER BY k NULLS FIRST), lag(k, 1, 0) OVER (ORDER BY k NULLS FIRST) FROM t1 GROUP BY ROLLUP(1)",
        },
        SqlTestCase {
            name: "rollup_cte_nested_window_types",
            description: "Window return types include nested nullable key elements for CTE consumers.",
            setup_sqls: &["CREATE TABLE t1(a Int32)"],
            sql: "WITH x AS (SELECT a, first_value([a]) OVER (ORDER BY a NULLS LAST) AS v FROM t1 GROUP BY ROLLUP(a)) SELECT x1.a, x1.v[1], x2.v[1] FROM x x1 JOIN x x2 ON x1.a IS NOT DISTINCT FROM x2.a",
        },
        SqlTestCase {
            name: "rollup_cte_nested_projection_types",
            description: "Projection types include nullable array elements for CTE consumers.",
            setup_sqls: &["CREATE TABLE t1(a Int32)"],
            sql: "WITH x AS (SELECT a, [a] AS v FROM t1 GROUP BY ROLLUP(a)) SELECT x1.a, x1.v[1], x2.v[1] FROM x x1 JOIN x x2 ON x1.a IS NOT DISTINCT FROM x2.a",
        },
        SqlTestCase {
            name: "rollup_cte_nested_subquery_types",
            description: "A correlated scalar subquery has nested nullable output types through a materialized CTE.",
            setup_sqls: &["CREATE TABLE t1(a Int32)"],
            sql: "WITH x AS (SELECT a, (SELECT [t1.a]) AS v FROM t1 GROUP BY ROLLUP(a)) SELECT x1.a, x1.v[1], x2.v[1] FROM x x1 JOIN x x2 ON x1.a IS NOT DISTINCT FROM x2.a",
        },
        SqlTestCase {
            name: "rollup_cte_references",
            description: "Both materialized CTE consumers preserve the nullable grouping key schema.",
            setup_sqls: &["CREATE TABLE t1(a Int32 NOT NULL)"],
            sql: "WITH x AS (SELECT a FROM t1 GROUP BY ROLLUP(a)) SELECT x1.a, x2.a FROM x x1 JOIN x x2 ON x1.a = x2.a",
        },
    ] {
        write_optimized_case(&mut file, &case, false).await?;
    }

    Ok(())
}

const ROLLUP_TABLE: &str = "CREATE TABLE t
(
    a UInt64,
    b UInt64,
    c UInt64,
    v UInt64
)";

const ROLLUP_QUERY: &str = "SELECT a, b, c, sum(v), count(v), min(v), max(v)
FROM t
GROUP BY ROLLUP(a, b, c)";
