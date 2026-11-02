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
// `p` is the large probe table.
const PROBE_TABLE: &str = "CREATE TABLE p(k BIGINT NOT NULL, v BIGINT NOT NULL)";
const LEFT_RANGE_TABLE: &str = "CREATE TABLE a(k BIGINT NOT NULL)";
const RIGHT_RANGE_TABLE: &str = "CREATE TABLE b(k BIGINT NOT NULL)";
const SMALL_TABLE: &str = "CREATE TABLE d(k BIGINT NOT NULL, v BIGINT NOT NULL)";
const LARGE_TABLE: &str = "CREATE TABLE m(k BIGINT NOT NULL)";
const NO_STATS_TABLE: &str = "CREATE TABLE u(k BIGINT NOT NULL)";

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
    ];
    for case in &distributed_cases {
        write_plan_case(&mut file, case, 2).await?;
    }

    let local_cases = [PlanCase {
        name: "left_join_does_not_build_underestimated_join",
        description: "CommuteJoin ranks an underestimated join by its bound, so the small table stays the build side.",
        sql: "SELECT j.k, d.v FROM (SELECT a.k FROM a JOIN b ON a.k = b.k) AS j LEFT JOIN d ON j.k = d.k",
        settings: &[],
    }];
    for case in &local_cases {
        write_plan_case(&mut file, case, 1).await?;
    }
    Ok(())
}
