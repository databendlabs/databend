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

// `a` and `b` are large, but their key ranges do not overlap, so a join of the
// two is estimated to return no rows. `d` is a small table with a unique key,
// `m` is a large table with a unique key, and `u` has no column statistics.
// `s` is a large table with a unique key whose NDV is below the size of its
// value domain, and `f` has the same statistics except that its NDV was filled
// from the row count. `p` is the large probe table. `r`, `w` and `e` record `t`
// in [1, 1000], so a range above 1000 is estimated to match no rows. `e` is `r`
// with an extra column `v` in [1, 1000] for a second predicate.
const PROBE_TABLE: &str = "CREATE TABLE p(k BIGINT NOT NULL, v BIGINT NOT NULL)";
const LEFT_RANGE_TABLE: &str = "CREATE TABLE a(k BIGINT NOT NULL)";
const RIGHT_RANGE_TABLE: &str = "CREATE TABLE b(k BIGINT NOT NULL)";
const SMALL_TABLE: &str = "CREATE TABLE d(k BIGINT NOT NULL, v BIGINT NOT NULL)";
const LARGE_TABLE: &str = "CREATE TABLE m(k BIGINT NOT NULL)";
const NO_STATS_TABLE: &str = "CREATE TABLE u(k BIGINT NOT NULL)";
const SPARSE_KEY_TABLE: &str = "CREATE TABLE s(k BIGINT NOT NULL)";
const FALLBACK_NDV_TABLE: &str = "CREATE TABLE f(k BIGINT NOT NULL)";
const LARGE_RANGE_TABLE: &str = "CREATE TABLE r(k BIGINT NOT NULL, t BIGINT NOT NULL)";
const MEDIUM_RANGE_TABLE: &str = "CREATE TABLE w(k BIGINT NOT NULL, t BIGINT NOT NULL)";
const LARGE_RANGE_EQUALITY_TABLE: &str =
    "CREATE TABLE e(k BIGINT NOT NULL, t BIGINT NOT NULL, v BIGINT NOT NULL)";

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
    settings: &'static [(&'static str, &'static str)],
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

async fn create_context(cluster_node_num: u64) -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create().await?;
    if cluster_node_num > 1 {
        ctx.set_cluster_node_num(cluster_node_num);
        ctx.set_table_warehouse_distribution(true);
    }
    let tables = [
        (
            PROBE_TABLE,
            1_000_000_000,
            HashMap::from([
                ("k".to_string(), int_stat(1, 1_000_000, 1_000_000)),
                ("v".to_string(), int_stat(1, 100, 100)),
            ]),
        ),
        (
            LEFT_RANGE_TABLE,
            10_000_000,
            HashMap::from([("k".to_string(), int_stat(1, 1000, 1000))]),
        ),
        (
            RIGHT_RANGE_TABLE,
            10_000_000,
            HashMap::from([("k".to_string(), int_stat(2001, 3000, 1000))]),
        ),
        (
            SMALL_TABLE,
            1000,
            HashMap::from([
                ("k".to_string(), int_stat(1, 1000, 1000)),
                ("v".to_string(), int_stat(1, 100, 100)),
            ]),
        ),
        (
            LARGE_TABLE,
            200_000_000,
            HashMap::from([("k".to_string(), int_stat(1, 200_000_000, 200_000_000))]),
        ),
        (NO_STATS_TABLE, 1_000_000, HashMap::new()),
        (
            SPARSE_KEY_TABLE,
            200_000_000,
            HashMap::from([("k".to_string(), int_stat(1, 1_000_000_000, 200_000_000))]),
        ),
        (
            FALLBACK_NDV_TABLE,
            200_000_000,
            HashMap::from([("k".to_string(), BasicColumnStatistics {
                ndv_is_inferred: true,
                ..int_stat(1, 1_000_000_000, 200_000_000)
            })]),
        ),
        (
            LARGE_RANGE_TABLE,
            1_000_000_000,
            HashMap::from([
                ("k".to_string(), int_stat(1, 1_000_000, 1_000_000)),
                ("t".to_string(), int_stat(1, 1000, 1000)),
            ]),
        ),
        (
            MEDIUM_RANGE_TABLE,
            10_000_000,
            HashMap::from([
                ("k".to_string(), int_stat(1, 1_000_000, 1_000_000)),
                ("t".to_string(), int_stat(1, 1000, 1000)),
            ]),
        ),
        (
            LARGE_RANGE_EQUALITY_TABLE,
            1_000_000_000,
            HashMap::from([
                ("k".to_string(), int_stat(1, 1_000_000, 1_000_000)),
                ("t".to_string(), int_stat(1, 1000, 1000)),
                ("v".to_string(), int_stat(1, 1000, 1000)),
            ]),
        ),
    ];
    for (sql, rows, column_stats) in tables {
        ctx.register_table_sql_with_stats(
            sql,
            Some(table_statistics(rows)),
            column_stats,
            HashMap::new(),
        )
        .await?;
    }
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
    let ctx = create_context(1).await?;
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
    writeln!(file, "max_cardinality: {:.3}", stats.max_cardinality)?;
    writeln!(
        file,
        "statistics_zero_bound: {}",
        stats.statistics_zero_bound
    )?;
    writeln!(file)?;
    Ok(())
}

async fn write_plan_case(
    file: &mut impl Write,
    case: &PlanCase,
    cluster_node_num: u64,
) -> Result<()> {
    let ctx = create_context(cluster_node_num).await?;
    let settings = ctx.get_settings();
    for (name, value) in case.settings {
        settings.set_setting(name.to_string(), value.to_string())?;
    }
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    writeln!(file, "cluster nodes: {cluster_node_num}")?;
    writeln!(file, "settings: {:?}", case.settings)?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_broadcast_build_bound_statistics_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "broadcast_build_bound_statistics.txt")?;
    let cases = [
        StatsCase {
            name: "scan",
            description: "A scan bounds its output by its own estimate.",
            sql: "SELECT * FROM a",
            operator: RelOp::Scan,
        },
        StatsCase {
            name: "inner_join_disjoint_ranges",
            description: "A join estimated to return no rows keeps the largest input as its bound.",
            sql: "SELECT * FROM a JOIN b ON a.k = b.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "union_all_of_joins",
            description: "UNION ALL adds the bounds of its inputs.",
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k UNION ALL SELECT a.k FROM a JOIN b ON a.k = b.k",
            operator: RelOp::UnionAll,
        },
        StatsCase {
            name: "aggregate_over_join",
            description: "An aggregate whose group keys have statistics resets the bound to its own estimate.",
            sql: "SELECT a.k, count(*) FROM a JOIN d ON a.k = d.k GROUP BY a.k",
            operator: RelOp::Aggregate,
        },
        StatsCase {
            name: "aggregate_without_group_statistics",
            description: "An aggregate whose group keys have no statistics keeps its input bound.",
            sql: "SELECT a.k, count(*) FROM a JOIN b ON a.k = b.k GROUP BY a.k",
            operator: RelOp::Aggregate,
        },
        StatsCase {
            name: "limit_over_join",
            description: "LIMIT caps the bound of an underestimated input at the limit.",
            sql: "SELECT * FROM a JOIN b ON a.k = b.k LIMIT 100",
            operator: RelOp::Limit,
        },
        StatsCase {
            name: "project_set_over_join",
            description: "A set-returning function expands the bound by the same factor as the estimate.",
            sql: "SELECT unnest([1, 2]) FROM a JOIN b ON a.k = b.k",
            operator: RelOp::ProjectSet,
        },
        StatsCase {
            name: "left_semi_join",
            description: "A semi join is bounded by the side it returns.",
            sql: "SELECT * FROM d LEFT SEMI JOIN a ON d.k = a.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "left_any_join",
            description: "LEFT ANY JOIN returns one row per left row, so it is bounded by the left side.",
            sql: "SELECT * FROM d LEFT ANY JOIN a ON d.k = a.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "right_any_join",
            description: "RIGHT ANY JOIN returns one row per right row, so it is bounded by the right side.",
            sql: "SELECT * FROM a RIGHT ANY JOIN d ON a.k = d.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "full_join",
            description: "A full join can return the unmatched rows of both inputs, so its bound covers their sum even when the estimate expects every row to match.",
            sql: "SELECT * FROM a FULL JOIN d ON a.k = d.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "cross_join_over_join",
            description: "A cross join returns every pair of input rows, so its bound is the product of the input bounds.",
            sql: "SELECT * FROM (SELECT a.k FROM a JOIN b ON a.k = b.k) AS j CROSS JOIN d",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_unique_key_missing_peer_stats",
            description: "A unique key caps the expected output at the rows of the side without statistics.",
            sql: "SELECT * FROM d JOIN u ON d.k = u.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "left_join_unique_key_missing_peer_stats",
            description: "A unique key on the null-extended side caps the expected output at the preserved rows.",
            sql: "SELECT * FROM u LEFT JOIN d ON u.k = d.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_repeated_key_missing_peer_stats",
            description: "A key with repeated values does not cap the expected output.",
            sql: "SELECT * FROM a JOIN u ON a.k = u.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_confident_unique_key",
            description: "A unique key whose NDV is below its domain size bounds the output by the peer side.",
            sql: "SELECT * FROM d JOIN s ON d.k = s.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_confident_unique_key_below_filter",
            description: "A filter keeps a collected NDV, so its unique key still bounds the output.",
            sql: "SELECT * FROM d JOIN (SELECT k FROM s WHERE k > 10) AS t ON d.k = t.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "observed_ndv_after_one_to_one_join",
            description: "A join on keys that are unique on both sides keeps the observed NDV of its output keys.",
            sql: "SELECT * FROM (SELECT x.k FROM s AS x JOIN s AS y ON x.k = y.k) AS j JOIN d ON j.k = d.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "observed_ndv_dropped_after_fan_out_join",
            description: "A join that may repeat the rows of `s` drops its observed NDV, so the key does not bound the next join.",
            sql: "SELECT * FROM (SELECT s.k FROM s JOIN u ON s.k = u.k) AS j JOIN d ON j.k = d.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_unique_key_with_fallback_ndv",
            description: "An NDV filled from the row count caps only the expected output.",
            sql: "SELECT * FROM d JOIN f ON d.k = f.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "inner_join_unique_key_at_domain_size",
            description: "A unique key whose NDV equals its domain size caps only the expected output.",
            sql: "SELECT * FROM d JOIN m ON d.k = m.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "left_join_confident_unique_key",
            description: "A confident unique key on the null-extended side bounds the output by the preserved side.",
            sql: "SELECT * FROM u LEFT JOIN s ON u.k = s.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "full_join_confident_unique_key",
            description: "A FULL join keeps the largest input bound.",
            sql: "SELECT * FROM u FULL JOIN s ON u.k = s.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "filter_range_outside_statistics",
            description: "A range outside the recorded min/max expects 1/2500 of the input and keeps the 1/5 unknown-filter bound.",
            sql: "SELECT * FROM r WHERE t > 1000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_outside_statistics_and_equality",
            description: "A selective predicate in the same conjunction lowers the expectation and the bound, in which only the range counts as an unknown filter.",
            sql: "SELECT * FROM r WHERE t > 1000 AND k = 5",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_equality_outside_statistics",
            description: "An equality outside the recorded min/max is still estimated as no rows.",
            sql: "SELECT * FROM r WHERE t = 2000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_inside_statistics",
            description: "A range inside the recorded min/max is bounded by its estimate.",
            sql: "SELECT * FROM r WHERE t > 999",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "eval_scalar_over_range_outside_statistics",
            description: "A projection keeps the rows of its input and the source of their bound.",
            sql: "SELECT k + 1 AS x FROM r WHERE t > 1000",
            operator: RelOp::EvalScalar,
        },
        StatsCase {
            name: "filter_over_range_outside_statistics",
            description: "A filter keeps the bound of a range outside the recorded min/max below it.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE k > 10",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_equality_over_range_outside_statistics",
            description: "A selective equality above a range outside the recorded min/max lowers the expected rows but keeps the bound of the range below.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE k = 5",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_over_range_outside_statistics_same_column",
            description: "A second range outside the recorded min/max on the same column above the first one keeps the bound of the first.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE t > 2000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_two_ranges_outside_statistics_same_column",
            description: "Two ranges outside the recorded min/max on the same column in one filter count as one unknown filter in the bound.",
            sql: "SELECT * FROM r WHERE t > 1000 AND t > 2000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_and_equality_over_range_outside_statistics_same_column",
            description: "Above a range outside the recorded min/max, a filter with a second range on the same column and a selective equality keeps the bound of the range below.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE t > 2000 AND k = 5",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_two_ranges_outside_statistics_same_column_and_equality",
            description: "Two ranges outside the recorded min/max on the same column and a selective equality in one filter: the equality on another column lowers the bound.",
            sql: "SELECT * FROM r WHERE t > 1000 AND t > 2000 AND k = 5",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_inside_statistics_over_range_outside_statistics_same_column",
            description: "A range inside the recorded min/max on the same column above a range outside it is estimated from statistics that the rows below no longer follow, so it keeps the bound of the range below.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE t > 999",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_equality_outside_statistics_over_range_outside_statistics_same_column",
            description: "An equality outside the recorded min/max on the same column above a range outside it is estimated as no rows, but keeps the bound of the range below.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE t = 2000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_outside_and_inside_statistics_same_column",
            description: "A range inside the recorded min/max next to a range outside it on the same column does not lower the bound of the conjunction.",
            sql: "SELECT * FROM r WHERE t > 1000 AND t > 999",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_outside_statistics_linked_column",
            description: "A predicate on a column that another predicate links to a range outside the recorded min/max does not lower the bound of the conjunction.",
            sql: "SELECT * FROM e WHERE t > 1000 AND t = v AND v > 999",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_over_window_over_range_outside_statistics",
            description: "A window function keeps the rows of its input, so a filter on its result keeps the bound of the range below.",
            sql: "SELECT * FROM (SELECT k, row_number() OVER (PARTITION BY k ORDER BY t) AS n FROM r WHERE t > 1000) AS x WHERE n <> 5",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_proven_empty_over_range_outside_statistics",
            description: "A filter proven empty bounds its output at zero rows.",
            sql: "SELECT * FROM (SELECT * FROM r WHERE t > 1000) AS x WHERE k IS NULL",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_two_ranges_outside_statistics",
            description: "Two ranges outside the recorded min/max each count as an unknown filter in the bound.",
            sql: "SELECT * FROM e WHERE t > 1000 AND v > 1000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "filter_range_outside_statistics_over_join",
            description: "A range outside the recorded min/max above a join whose bound exceeds its estimate keeps the strict row limit.",
            sql: "SELECT * FROM d JOIN r ON d.k = r.k WHERE r.t > 1000",
            operator: RelOp::Filter,
        },
        StatsCase {
            name: "join_over_range_outside_statistics",
            description: "A join derives its own bound, which may include fan-out.",
            sql: "SELECT * FROM d JOIN (SELECT * FROM r WHERE t > 1000) AS x ON d.k = x.k",
            operator: RelOp::Join,
        },
        StatsCase {
            name: "limit_over_range_outside_statistics",
            description: "LIMIT caps the bound and keeps the strict row limit.",
            sql: "SELECT * FROM r WHERE t > 1000 LIMIT 300000000",
            operator: RelOp::Limit,
        },
    ];

    for case in &cases {
        write_stats_case(&mut file, case).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_broadcast_build_bound_plan_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "broadcast_build_bound_plan.txt")?;
    const NO_REORDER: &[(&str, &str)] = &[("disable_join_reorder", "1")];
    let distributed_cases = [
        PlanCase {
            name: "small_build_is_broadcast",
            description: "A small, reliably estimated build side is broadcast.",
            sql: "SELECT p.v, d.v FROM p JOIN d ON p.k = d.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "underestimated_join_build_is_shuffled",
            description: "A build side whose estimate is far below its input bound is not broadcast.",
            sql: "SELECT p.v FROM p JOIN (SELECT a.k FROM a JOIN b ON a.k = b.k) AS j ON p.k = j.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "enforced_broadcast_of_underestimated_join_build",
            description: "enforce_broadcast_join still broadcasts an underestimated build side.",
            sql: "SELECT p.v FROM p JOIN (SELECT a.k FROM a JOIN b ON a.k = b.k) AS j ON p.k = j.k",
            settings: &[
                ("disable_join_reorder", "1"),
                ("enforce_broadcast_join", "1"),
            ],
        },
        PlanCase {
            name: "build_over_row_limit_is_shuffled",
            description: "A build side above max_broadcast_join_build_rows is not broadcast.",
            sql: "SELECT p.v FROM p JOIN m ON p.k = m.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "zero_row_limit_disables_limit",
            description: "max_broadcast_join_build_rows = 0 disables the row limit.",
            sql: "SELECT p.v FROM p JOIN m ON p.k = m.k",
            settings: &[
                ("disable_join_reorder", "1"),
                ("max_broadcast_join_build_rows", "0"),
            ],
        },
        PlanCase {
            name: "serial_underestimated_build_is_broadcast",
            description: "A Serial build is broadcast within the row limit even when underestimated, because not broadcasting it would make the whole join Serial.",
            sql: "SELECT p.v FROM p JOIN (SELECT a.k FROM a JOIN b ON a.k = b.k LIMIT 1000) AS j ON p.k = j.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "serial_build_over_row_limit_stays_serial",
            description: "A Serial build whose bound exceeds max_broadcast_join_build_rows is not broadcast, although its estimate is 0.",
            sql: "SELECT p.v FROM p JOIN (SELECT a.k FROM a JOIN b ON a.k = b.k LIMIT 1000) AS j ON p.k = j.k",
            settings: &[
                ("disable_join_reorder", "1"),
                ("max_broadcast_join_build_rows", "500"),
            ],
        },
        PlanCase {
            name: "confident_unique_key_join_build_is_broadcast",
            description: "A join on a confident unique key is bounded by its small peer side, so it can be broadcast.",
            sql: "SELECT p.v FROM p JOIN (SELECT d.k FROM d JOIN s ON d.k = s.k) AS j ON p.k = j.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "fallback_ndv_join_build_is_shuffled",
            description: "A join on a key whose NDV was filled from the row count keeps the large input bound, so it is not broadcast.",
            sql: "SELECT p.v FROM p JOIN (SELECT d.k FROM d JOIN f ON d.k = f.k) AS j ON p.k = j.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "unique_key_at_domain_size_join_build_is_shuffled",
            description: "A join on a key whose NDV equals its domain size keeps the large input bound, so it is not broadcast.",
            sql: "SELECT p.v FROM p JOIN (SELECT d.k FROM d JOIN m ON d.k = m.k) AS j ON p.k = j.k",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "range_outside_statistics_build_is_broadcast",
            description: "A build side filtered by a range outside the recorded min/max is broadcast when its expected rows are within max_broadcast_join_build_rows, although its unknown-filter bound exceeds the limit.",
            sql: "SELECT p.v FROM p JOIN r ON p.k = r.k WHERE r.t > 1000",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "range_outside_statistics_or_build_is_broadcast",
            description: "An OR with a range outside the recorded min/max keeps the range's bound source, so the build side is broadcast by its expected rows.",
            sql: "SELECT p.v FROM p JOIN r ON p.k = r.k WHERE r.t > 1000 OR r.k = 5",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "range_outside_statistics_expected_rows_over_row_limit_is_shuffled",
            description: "A build side filtered by a range outside the recorded min/max is not broadcast when its expected rows exceed max_broadcast_join_build_rows.",
            sql: "SELECT p.v FROM p JOIN r ON p.k = r.k WHERE r.t > 1000",
            settings: &[
                ("disable_join_reorder", "1"),
                ("max_broadcast_join_build_rows", "100000"),
            ],
        },
        PlanCase {
            name: "range_outside_statistics_and_equality_build_is_broadcast",
            description: "A selective equality next to a range outside the recorded min/max lowers the expected rows and the bound alike, so the build side is broadcast.",
            sql: "SELECT p.v FROM p JOIN e ON p.k = e.k WHERE e.t > 1000 AND e.v = 5",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "two_ranges_outside_statistics_severe_underestimate_is_shuffled",
            description: "Two ranges outside the recorded min/max each count as an unknown filter in the bound, which stays far above the expected rows, so the build side is not broadcast.",
            sql: "SELECT p.v FROM p JOIN e ON p.k = e.k WHERE e.t > 1000 AND e.v > 1000",
            settings: NO_REORDER,
        },
        PlanCase {
            name: "range_outside_statistics_build_within_limit_is_broadcast",
            description: "A build side filtered by a range outside the recorded min/max is broadcast when its unknown-filter bound is within max_broadcast_join_build_rows.",
            sql: "SELECT p.v FROM p JOIN w ON p.k = w.k WHERE w.t > 1000",
            settings: NO_REORDER,
        },
    ];
    for case in &distributed_cases {
        write_plan_case(&mut file, case, 2).await?;
    }

    let local_cases = [
        PlanCase {
            name: "left_join_does_not_build_underestimated_join",
            description: "CommuteJoin ranks an underestimated join by its bound, so the small table stays the build side.",
            sql: "SELECT j.k, d.v FROM (SELECT a.k FROM a JOIN b ON a.k = b.k) AS j LEFT JOIN d ON j.k = d.k",
            settings: &[],
        },
        PlanCase {
            name: "left_join_builds_range_outside_statistics",
            description: "CommuteJoin ranks a range outside the recorded min/max by its small expected rows, so it becomes the build side instead of the larger table.",
            sql: "SELECT r.k, a.k FROM r LEFT JOIN a ON r.k = a.k WHERE r.t > 1000",
            settings: &[],
        },
        PlanCase {
            name: "left_join_builds_range_outside_statistics_and_equality",
            description: "A selective equality next to a range outside the recorded min/max keeps the bound within 1000x of the small expected rows, so CommuteJoin still makes this side the build side.",
            sql: "SELECT e.k, a.k FROM e LEFT JOIN a ON e.k = a.k WHERE e.t > 1000 AND e.v = 5",
            settings: &[],
        },
    ];
    for case in &local_cases {
        write_plan_case(&mut file, case, 1).await?;
    }
    Ok(())
}
