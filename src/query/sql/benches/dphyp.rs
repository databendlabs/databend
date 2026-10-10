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

//! Independent regression workloads, organized by three interacting costs:
//! search/enumeration, constructing and estimating a candidate, and selecting
//! among candidates (including replacing/freeing candidate state).
//! These are observation lenses, not orthogonal workload parameters. Each case
//! owns its SQL; graph shape alone does not establish its candidate count.
//! See README.md for the implementation mapping and interpretation limits.
//! Only fixture preparation, cache isolation and timing are shared.
//!
//! Run: cargo bench -p databend-common-sql --bench dphyp
//! Smoke: cargo bench -p databend-common-sql --bench dphyp -- --test
//! List/filter cases using the usual divan CLI (--help).
//! Compare the same named case across revisions, with the same toolchain,
//! machine and build profile. Absolute timings are not pass/fail assertions.
//! No service, table data, binding or statistics collection is measured.
//! max_time limits sampling, not a single optimizer invocation; expensive
//! cases can exceed it. Use the optimized bench profile for comparisons.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::optimizers::DPhpyOptimizer;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::plans::Plan;
use databend_common_sql_test_support::LiteTableContext;
use databend_common_statistics::Datum;
use divan::Bencher;
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    })
}

struct Case {
    plan: SExpr,
    opt_ctx: Arc<OptimizerContext>,
}

impl Case {
    // Overrides are concrete fixture data: (table index, rows, key/value NDV).
    fn new(sql: &str, stats_overrides: &[(usize, u64, u64)]) -> Self {
        runtime().block_on(async {
            let ctx = LiteTableContext::create().await.unwrap();
            for (name, value) in [("enable_dphyp", "1"), ("disable_join_reorder", "0")] {
                ctx.get_settings()
                    .set_setting(name.to_string(), value.to_string())
                    .unwrap();
            }
            // A common catalog, not a parameter controlling a case's shape.
            // Unused tables do not participate in its bound plan.
            for table in 0..40 {
                let (rows, ndv) = stats_overrides
                    .iter()
                    .find(|(index, _, _)| *index == table)
                    .map(|(_, rows, ndv)| (*rows, *ndv))
                    .unwrap_or((10_000 + table as u64 * 100, 10_000));
                let stats = TableStatistics {
                    num_rows: Some(rows),
                    data_size: Some(rows * 16),
                    number_of_blocks: Some(1),
                    number_of_segments: Some(1),
                    ..Default::default()
                };
                let columns = ["k", "v"]
                    .into_iter()
                    .map(|name| {
                        (name.to_string(), BasicColumnStatistics {
                            min: Some(Datum::Int(0)),
                            max: Some(Datum::Int(9999)),
                            ndv: Some(ndv),
                            null_count: 0,
                            in_memory_size: rows * 8,
                        })
                    })
                    .collect();
                ctx.register_table_sql_with_stats(
                    &format!("CREATE TABLE t{table} (k BIGINT NOT NULL, v BIGINT NOT NULL)"),
                    Some(stats),
                    columns,
                    HashMap::new(),
                )
                .await
                .unwrap();
            }
            let bound = ctx.bind_sql(sql).await.unwrap();
            let Plan::Query {
                s_expr, metadata, ..
            } = bound
            else {
                unreachable!("benchmark input must bind to a query")
            };
            let opt_ctx =
                OptimizerContext::new(ctx.clone(), metadata, ctx.get_function_context().unwrap())
                    .with_settings(ctx.get_settings().as_ref())
                    .unwrap();
            let plan = CollectStatisticsOptimizer::new(opt_ctx.clone())
                .optimize(*s_expr)
                .await
                .unwrap();
            // Only remove identity projections. Full rewrite would change the
            // graph and merge predicates before the phase we want to measure.
            let plan = RecursiveRuleOptimizer::new(opt_ctx.clone(), &[RuleID::EliminateEvalScalar])
                .optimize_sync(plan)
                .unwrap();
            Case { plan, opt_ctx }
        })
    }
}

/// Cloning SExpr would share OnceLock caches with preceding samples. Rebuild
/// every node, retaining collected Scan statistics but clearing derived caches.
fn fresh_plan(expr: &SExpr) -> SExpr {
    expr.replace_children(expr.children().map(|child| Arc::new(fresh_plan(child))))
}

/// Avoid a deep left-associated SQL AND tree spending excessive time in the
/// unmeasured type-check/constant-folding setup. This preserves the predicate
/// bank; it does not rewrite the bound plan or change the measured algorithm.
fn conjunction(predicates: &[String]) -> String {
    match predicates {
        [predicate] => predicate.clone(),
        [] => unreachable!("benchmark conjunction must not be empty"),
        _ => {
            let middle = predicates.len() / 2;
            format!(
                "({}) AND ({})",
                conjunction(&predicates[..middle]),
                conjunction(&predicates[middle..])
            )
        }
    }
}

fn bench_sql(bencher: Bencher, sql: &str) {
    bench_case(bencher, Case::new(sql, &[]));
}

fn bench_case(bencher: Bencher, case: Case) {
    bencher
        .with_inputs(|| {
            (
                DPhpyOptimizer::new(case.opt_ctx.clone()),
                fresh_plan(&case.plan),
            )
        })
        .bench_local_values(|(mut optimizer, plan)| {
            divan::black_box(
                runtime()
                    .block_on(optimizer.optimize(divan::black_box(plan)))
                    .unwrap(),
            )
        });
}

#[divan::bench_group]
mod search {
    use super::*;

    /// Long, sparse input: detect excessive per-node work even when each relation
    /// has few edges. Keep the workload fixed rather than extrapolating small cases.
    #[divan::bench(max_time = 1)]
    fn long_equality_chain(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..40 {
            write!(sql, " JOIN t{right} ON t{}.k = t{right}.k", right - 1).unwrap();
        }
        bench_sql(bencher, &sql);
    }

    /// Many equality edges in a compact connected graph: stress edge matching,
    /// condition assembly and work performed before falling back to greedy search.
    #[divan::bench(max_time = 1)]
    fn dense_equality_edges(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..20 {
            write!(sql, " JOIN t{right} ON t0.k = t{right}.k").unwrap();
            for left in 1..right {
                write!(sql, " AND t{left}.k = t{right}.k").unwrap();
            }
        }
        bench_sql(bencher, &sql);
    }

    /// Nine relations leave the smaller-graph enumeration policy enabled. A hub
    /// provides many possible combinations despite the modest number of tables.
    #[divan::bench(max_time = 1)]
    fn hub_with_unrestricted_enumeration(bencher: Bencher) {
        bench_sql(
            bencher,
            "SELECT t0.k FROM t0
        JOIN t1 ON t0.k = t1.k
        JOIN t2 ON t0.k = t2.k
        JOIN t3 ON t0.k = t3.k
        JOIN t4 ON t0.k = t4.k
        JOIN t5 ON t0.k = t5.k
        JOIN t6 ON t0.k = t6.k
        JOIN t7 ON t0.k = t7.k
        JOIN t8 ON t0.k = t8.k",
        );
    }

    /// A concrete input at the enumeration-policy boundary. Track it independently
    /// so changes to pruning do not get hidden by averages over other workloads.
    #[divan::bench(max_time = 1)]
    fn hub_at_enumeration_boundary(bencher: Bencher) {
        bench_sql(
            bencher,
            "SELECT t0.k FROM t0
        JOIN t1 ON t0.k = t1.k
        JOIN t2 ON t0.k = t2.k
        JOIN t3 ON t0.k = t3.k
        JOIN t4 ON t0.k = t4.k
        JOIN t5 ON t0.k = t5.k
        JOIN t6 ON t0.k = t6.k
        JOIN t7 ON t0.k = t7.k
        JOIN t8 ON t0.k = t8.k
        JOIN t9 ON t0.k = t9.k",
        );
    }

    /// A small clique permits unrestricted enumeration, with many alternative
    /// splits of the same relation sets. Diagnose whether the DP budget is
    /// exhausted; do not infer fallback merely from the size of a query.
    #[divan::bench(max_time = 1)]
    fn small_clique_search_budget(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..9 {
            write!(sql, " JOIN t{right} ON t0.k = t{right}.k").unwrap();
            for left in 1..right {
                write!(sql, " AND t{left}.k = t{right}.k").unwrap();
            }
        }
        bench_sql(bencher, &sql);
    }

    /// Like the historical large_query sqllogictest: a large connected component
    /// followed by an unconnected relation, exercising cross-join completion.
    #[divan::bench(max_time = 1)]
    fn large_query_with_unconnected_relation(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..19 {
            write!(sql, " JOIN t{right} ON t{}.k = t{right}.k", right - 1).unwrap();
        }
        sql.push_str(" CROSS JOIN t19");
        bench_sql(bencher, &sql);
    }
}

#[divan::bench_group]
mod candidate {
    use super::*;

    /// Filters originate at many different input scopes but share the hub column.
    /// Reordering must scan/place them for candidate subtrees. Identity projections
    /// are removed during preparation; filters and OR expressions are not rewritten.
    #[divan::bench(max_time = 1)]
    fn residuals_from_nested_filter_scopes(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k, t0.v FROM t0");
        for right in 1..20 {
            sql = format!(
                "SELECT s.k, s.v FROM ({sql}) s JOIN t{right} ON s.k = t{right}.k
             WHERE s.v + t{right}.v > 100
               AND s.v + t{right}.v > 200
               AND ((s.v > 10 AND t{right}.v < 5000)
                    OR (s.v < 5000 AND t{right}.v > 10))"
            );
        }
        bench_sql(bencher, &sql);
    }

    /// Many predicates need all eight inputs. Most candidate subtrees cannot
    /// consume any of them, but placement still checks the collected list.
    #[divan::bench(max_time = 1)]
    fn pending_multi_relation_predicates(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..8 {
            write!(sql, " JOIN t{right} ON t0.k = t{right}.k").unwrap();
        }
        sql.push_str(" WHERE ");
        let predicates = (0..128)
            .map(|threshold| {
                format!("t0.v + t1.v + t2.v + t3.v + t4.v + t5.v + t6.v + t7.v > {threshold}")
            })
            .collect::<Vec<_>>();
        sql.push_str(&conjunction(&predicates));
        // Root Filters are not collected into the reorder region. Put this
        // one below an additional join so placement actually sees the bank.
        sql = format!("SELECT s.k FROM ({sql}) s JOIN t8 ON s.k = t8.k");
        bench_sql(bencher, &sql);
    }

    /// Many distinct expressions can be installed together at the same join.
    /// Exercise condition copying, linear uniqueness checks and estimation.
    #[divan::bench(max_time = 1)]
    fn many_residuals_on_one_join(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0 JOIN t1 ON t0.k = t1.k");
        for right in 2..7 {
            write!(sql, " JOIN t{right} ON t0.k = t{right}.k").unwrap();
        }
        sql.push_str(" WHERE ");
        let predicates = (0..192)
            .map(|threshold| format!("t0.v + t1.v > {threshold}"))
            .collect::<Vec<_>>();
        sql.push_str(&conjunction(&predicates));
        sql = format!("SELECT s.k FROM ({sql}) s JOIN t7 ON s.k = t7.k");
        bench_sql(bencher, &sql);
    }

    /// A single large predicate is a different cost from many small predicates:
    /// each branch crosses several inputs and the complete tree stays intact.
    #[divan::bench(max_time = 1)]
    fn wide_cross_side_or(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..6 {
            write!(sql, " JOIN t{right} ON t{}.k = t{right}.k", right - 1).unwrap();
        }
        sql.push_str(" WHERE ");
        for branch in 0..64 {
            if branch != 0 {
                sql.push_str(" OR ");
            }
            write!(sql,
                "(t0.v BETWEEN {branch} AND {} AND t1.v + t2.v > {branch} AND t3.v + t4.v > {branch} AND t5.v < {})",
                branch + 100, 9999 - branch,
            ).unwrap();
        }
        sql = format!("SELECT s.k FROM ({sql}) s JOIN t6 ON s.k = t6.k");
        bench_sql(bencher, &sql);
    }

    /// Q13-shaped input, including an already extracted necessary OR at a lower
    /// position. Exercise relocation of complex predicates rather than graph size.
    #[divan::bench(max_time = 1)]
    fn q13_derived_or_relocation(bencher: Bencher) {
        bench_sql(
            bencher,
            "SELECT t0.k FROM
        (SELECT t0.k, t0.v, t1.v AS category FROM t0 JOIN t1 ON t0.k = t1.k
         WHERE (t1.v = 1 AND t0.v BETWEEN 100 AND 150)
            OR (t1.v = 2 AND t0.v BETWEEN 50 AND 100)
            OR (t1.v = 3 AND t0.v BETWEEN 150 AND 200)) t0
        JOIN t2 ON t0.k = t2.k
        WHERE (t0.category = 1 AND t0.v BETWEEN 100 AND 150 AND t2.v = 3)
           OR (t0.category = 2 AND t0.v BETWEEN 50 AND 100 AND t2.v = 1)
           OR (t0.category = 3 AND t0.v BETWEEN 150 AND 200 AND t2.v = 1)",
        );
    }
}

#[divan::bench_group]
mod selection {
    use super::*;

    /// Symmetric estimates make several alternatives similarly priced. This
    /// targets the selection/discard path, not a claimed equal-cost count.
    #[divan::bench(max_time = 1)]
    fn symmetric_cycle(bencher: Bencher) {
        let sql = "SELECT t0.k FROM t0
            JOIN t1 ON t0.k = t1.k
            JOIN t2 ON t1.k = t2.k
            JOIN t3 ON t2.k = t3.k
            JOIN t4 ON t3.k = t4.k
            JOIN t5 ON t4.k = t5.k
            JOIN t6 ON t5.k = t6.k AND t6.k = t0.k";
        let stats = [
            (0, 10_000, 10_000),
            (1, 10_000, 10_000),
            (2, 10_000, 10_000),
            (3, 10_000, 10_000),
            (4, 10_000, 10_000),
            (5, 10_000, 10_000),
            (6, 10_000, 10_000),
        ];
        bench_case(bencher, Case::new(sql, &stats));
    }

    /// Many alternative partitions and asymmetric estimates expose state
    /// replacement/GC. Confirm replacements through separate diagnostics.
    #[divan::bench(max_time = 1)]
    fn dense_skewed_subplans(bencher: Bencher) {
        let mut sql = String::from("SELECT t0.k FROM t0");
        for right in 1..8 {
            write!(sql, " JOIN t{right} ON t0.k = t{right}.k").unwrap();
            for left in 1..right {
                write!(sql, " AND t{left}.k = t{right}.k").unwrap();
            }
        }
        let stats = [
            (0, 50_000_000, 10_000),
            (1, 100, 100),
            (2, 5_000_000, 10_000),
            (3, 10_000, 500),
            (4, 500_000, 10_000),
            (5, 10, 10),
            (6, 1_000_000, 1000),
            (7, 1000, 100),
        ];
        bench_case(bencher, Case::new(&sql, &stats));
    }

    /// Selective inputs occur late in the bound tree. Better alternatives can
    /// change which DP states survive and hence arena reclamation costs. Actual
    /// replacement counts require instrumentation; skew alone is not proof.
    #[divan::bench(max_time = 1)]
    fn late_selective_inputs(bencher: Bencher) {
        let sql = "SELECT t0.k FROM t0
            JOIN t1 ON t0.k = t1.k
            JOIN t2 ON t0.k = t2.k
            JOIN t3 ON t1.k = t3.k
            JOIN t4 ON t2.k = t4.k
            JOIN t5 ON t3.k = t5.k AND t4.k = t5.k
            JOIN t6 ON t0.k = t6.k";
        let stats = [
            (0, 1_000_000, 10_000),
            (1, 100_000, 10_000),
            (2, 500_000, 10_000),
            (3, 50_000, 10_000),
            (4, 10_000, 10_000),
            (5, 100, 100),
            (6, 10, 10),
        ];
        bench_case(bencher, Case::new(sql, &stats));
    }
}
