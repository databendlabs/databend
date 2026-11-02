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

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::DefaultPhysicalPlanner;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOp;
use databend_common_statistics::Datum;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

// `e` is exactly empty. `t` and `w` have rows, and their column statistics are
// only used for estimation. The harness evaluates `now()` and `today()` at the
// Unix epoch while the statistics of `w` cover 2020, so a window such as
// `ts >= '1969-01-01 00:00:00' AND ts < now()` is satisfiable on its own but
// contradicts the column statistics. Every row of `z.n` is NULL, and both the
// row count and the null count of `z` are exact.
const EMPTY_TABLE: &str = "CREATE TABLE e(k BIGINT NOT NULL, v BIGINT NOT NULL)";
const TABLE: &str = "CREATE TABLE t(k BIGINT NOT NULL, v BIGINT NOT NULL)";
const TIME_TABLE: &str = "CREATE TABLE w(ts TIMESTAMP NOT NULL, d DATE NOT NULL)";
const ALL_NULL_TABLE: &str = "CREATE TABLE z(k BIGINT NOT NULL, n BIGINT NULL)";

struct StatsCase {
    name: &'static str,
    description: &'static str,
    sql: &'static str,
    operator: RelOp,
}

struct PlanCase {
    name: &'static str,
    description: &'static str,
    sql: &'static str,
}

fn int_stat(min: i64, max: i64, ndv: u64) -> BasicColumnStatistics {
    BasicColumnStatistics {
        min: Some(Datum::Int(min)),
        max: Some(Datum::Int(max)),
        ndv: Some(ndv),
        null_count: 0,
        in_memory_size: 8000,
        ndv_is_inferred: false,
    }
}

async fn create_context() -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        EMPTY_TABLE,
        Some(table_statistics(0)),
        HashMap::new(),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        TABLE,
        Some(table_statistics(1000)),
        HashMap::from([
            ("k".to_string(), int_stat(1, 1000, 1000)),
            ("v".to_string(), int_stat(1, 100, 100)),
        ]),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        ALL_NULL_TABLE,
        Some(table_statistics(1000)),
        HashMap::from([
            ("k".to_string(), int_stat(1, 1000, 1000)),
            ("n".to_string(), BasicColumnStatistics {
                min: None,
                max: None,
                ndv: Some(0),
                null_count: 1000,
                in_memory_size: 1000,
                ndv_is_inferred: false,
            }),
        ]),
        HashMap::new(),
    )
    .await?;
    // `ts` covers 2020 in microseconds and `d` covers 2020 in days.
    ctx.register_table_sql_with_stats(
        TIME_TABLE,
        Some(table_statistics(1000)),
        HashMap::from([
            (
                "ts".to_string(),
                int_stat(1_577_836_800_000_000, 1_609_459_199_000_000, 1000),
            ),
            ("d".to_string(), int_stat(18_262, 18_627, 366)),
        ]),
        HashMap::new(),
    )
    .await?;
    Ok(ctx)
}

fn find_operator(expr: &PExpr, operator: RelOp) -> Option<&PExpr> {
    if expr.plan().rel_op() == operator {
        return Some(expr);
    }
    expr.children()
        .find_map(|child| find_operator(child, operator.clone()))
}

// Derives statistics on the bound plan with collected table statistics, so the
// operator under test is not rewritten by other optimizer rules.
async fn write_stats_case(file: &mut impl Write, case: &StatsCase) -> Result<()> {
    let ctx = create_context().await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(case.sql).await?
    else {
        return Err(ErrorCode::Internal("expected query plan"));
    };
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata, ctx.get_function_context()?);
    let mut collector = CollectStatisticsOptimizer::new(opt_ctx);
    let s_expr = collector.optimize((*s_expr).into_logical()?).await?;
    let p_expr = DefaultPhysicalPlanner::plan(&s_expr)?;
    let target = find_operator(&p_expr, case.operator.clone()).ok_or_else(|| {
        ErrorCode::Internal(format!("cannot find {:?} in bound plan", case.operator))
    })?;
    let stats = RelExpr::with_p_expr(target).derive_cardinality(&ctx.stat_context()?)?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    writeln!(file, "operator: {:?}", case.operator)?;
    writeln!(file, "cardinality: {:.3}", stats.cardinality)?;
    writeln!(
        file,
        "precise_cardinality: {:?}",
        stats.statistics.precise_cardinality
    )?;
    writeln!(file)?;
    Ok(())
}

async fn write_plan_case(file: &mut impl Write, case: &PlanCase) -> Result<()> {
    let ctx = create_context().await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_proven_empty_statistics_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "proven_empty_statistics.txt")?;
    let cases = [
        StatsCase {
            name: "filter_over_empty_input",
            description: "A filter over an exactly empty input is proven empty.",
            sql: "SELECT k FROM e WHERE v > 10",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "constant_false_filter",
            description: "A predicate that is false for every value is proven empty.",
            sql: "SELECT k FROM t WHERE k > 10 AND k IS NULL",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "all_null_filter",
            description: "A comparison on a column whose exact null count covers every input row is proven empty.",
            sql: "SELECT k FROM z WHERE n > 10",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "all_null_filter_over_estimated_input",
            description: "The all-NULL fact needs an exact input row count, so it does not apply above an estimated filter.",
            sql: "SELECT k FROM (SELECT * FROM z WHERE k > 10) AS f WHERE n > 10",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "out_of_range_filter",
            description: "A zero estimate from column statistics stays imprecise.",
            sql: "SELECT k FROM t WHERE k > 5000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "now_window_filter",
            description: "A now() window that only contradicts column statistics stays imprecise.",
            sql: "SELECT ts FROM w WHERE ts >= '1969-01-01 00:00:00' AND ts < now()",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "today_window_filter",
            description: "A today() window that only contradicts column statistics stays imprecise.",
            sql: "SELECT d FROM w WHERE d >= '1969-01-01' AND d < today()",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "left_join_empty_preserved_side",
            description: "LEFT JOIN is proven empty when its preserved side is proven empty.",
            sql: "SELECT * FROM e LEFT JOIN t ON e.k = t.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "left_join_empty_null_extended_side",
            description: "LEFT JOIN keeps the preserved rows when only the other side is empty.",
            sql: "SELECT * FROM t LEFT JOIN e ON t.k = e.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "right_join_empty_preserved_side",
            description: "RIGHT JOIN is proven empty when its preserved side is proven empty.",
            sql: "SELECT * FROM t RIGHT JOIN e ON t.k = e.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_empty_side",
            description: "INNER JOIN is proven empty when either side is proven empty.",
            sql: "SELECT * FROM t INNER JOIN e ON t.k = e.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_empty_filtered_side",
            description: "A proven-empty filter propagates through INNER JOIN.",
            sql: "SELECT * FROM t INNER JOIN (SELECT * FROM e WHERE v > 10) AS f ON t.k = f.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_estimated_zero_side",
            description: "An estimated-zero side does not make INNER JOIN precise.",
            sql: "SELECT * FROM t INNER JOIN (SELECT * FROM t WHERE k > 5000) AS f ON t.k = f.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "full_join_one_empty_side",
            description: "FULL JOIN keeps the rows of the non-empty side.",
            sql: "SELECT * FROM t FULL JOIN e ON t.k = e.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "full_join_both_sides_empty",
            description: "FULL JOIN is proven empty only when both sides are proven empty.",
            sql: "SELECT * FROM e AS a FULL JOIN e AS b ON a.k = b.k",
            operator: RelOp::Join,
        },
    ];

    for case in &cases {
        write_stats_case(&mut file, case).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_proven_empty_plan_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "proven_empty_plan.txt")?;
    let cases = [
        PlanCase {
            name: "count_over_now_window",
            description: "COUNT(*) over a now() window must not be folded to a constant.",
            sql: "SELECT count(*) FROM w WHERE ts >= '1969-01-01 00:00:00' AND ts < now()",
        },
        PlanCase {
            name: "count_over_today_window",
            description: "COUNT(*) over a today() window must not be folded to a constant.",
            sql: "SELECT count(*) FROM w WHERE d >= '1969-01-01' AND d < today()",
        },
        PlanCase {
            name: "count_over_out_of_range_filter",
            description: "COUNT(*) over a statistics-derived zero estimate must not be folded.",
            sql: "SELECT count(*) FROM t WHERE k > 5000",
        },
        PlanCase {
            name: "count_over_all_null_filter",
            description: "COUNT(*) over a comparison on an all-NULL column with exact counts can be folded to 0.",
            sql: "SELECT count(*) FROM z WHERE n > 10",
        },
        PlanCase {
            name: "count_over_filtered_empty_table",
            description: "COUNT(*) over a filter on an exactly empty table can be folded to 0.",
            sql: "SELECT count(*) FROM e WHERE v > 10",
        },
        PlanCase {
            name: "left_join_builds_proven_empty_side",
            description: "On a zero-cardinality tie, LEFT JOIN is commuted so that the proven-empty side is built.",
            sql: "SELECT * FROM (SELECT * FROM e WHERE v > 10) AS a LEFT JOIN (SELECT * FROM t WHERE k > 5000) AS b ON a.k = b.k",
        },
        PlanCase {
            name: "right_join_keeps_proven_empty_build_side",
            description: "On a zero-cardinality tie, RIGHT JOIN is not commuted when the proven-empty side is already built.",
            sql: "SELECT * FROM (SELECT * FROM t WHERE k > 5000) AS a RIGHT JOIN (SELECT * FROM e WHERE v > 10) AS b ON a.k = b.k",
        },
        PlanCase {
            name: "left_join_builds_all_null_proven_empty_side",
            description: "On a zero-cardinality tie, a filter proven empty by an all-NULL column is built instead of an estimated-zero input.",
            sql: "SELECT * FROM (SELECT * FROM z WHERE n > 10) AS a LEFT JOIN (SELECT * FROM t WHERE k > 5000) AS b ON a.k = b.k",
        },
        PlanCase {
            name: "left_join_estimated_zero_tie",
            description: "A zero-cardinality tie between two estimates keeps the existing LEFT JOIN order.",
            sql: "SELECT * FROM (SELECT * FROM t WHERE k > 5000) AS a LEFT JOIN (SELECT * FROM t WHERE v > 500) AS b ON a.k = b.k",
        },
    ];

    for case in &cases {
        write_plan_case(&mut file, case).await?;
    }
    Ok(())
}
