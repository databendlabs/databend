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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::match_op;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::Distribution;
use databend_common_sql::optimizer::ir::ExprVisitor;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::PVisitAction;
use databend_common_sql::optimizer::ir::PatternExtractor;
use databend_common_sql::optimizer::ir::Physical;
use databend_common_sql::optimizer::ir::RequiredProperty;
use databend_common_sql::optimizer::optimize;
use databend_common_sql::optimizer::optimizers::CascadesOptimizer;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::Plan;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_late_split_preserves_aggregate_self_join_elimination() -> Result<()> {
    let mut file = open_golden_file("optimizer", "late_split_aggregate.txt")?;
    for (name, sql, expected_scans) in [
        ("eager_preparation_exposes_self_join",
         "SELECT raw.k, sum(raw.v) FROM t raw JOIN
          (SELECT k, sum(v) AS total FROM t GROUP BY k HAVING sum(v) > 10) strict
          ON raw.k = strict.k GROUP BY raw.k", 1),
        ("unsuccessful_eager_preparation_is_discarded",
         "SELECT raw.k, sum(raw.v) FROM t raw JOIN
          (SELECT k, sum(v) AS total FROM t WHERE v > 0 GROUP BY k HAVING sum(v) > 10) strict
          ON raw.k = strict.k GROUP BY raw.k", 2),
        ("same_rows_eliminate_loose_aggregate",
         "SELECT strict.k, strict.total, loose.total + 1 FROM
          (SELECT k, sum(v) AS total FROM t GROUP BY k HAVING sum(v) > 10) strict
          JOIN (SELECT k, sum(v) AS total FROM t GROUP BY k) loose ON strict.k = loose.k", 1),
        ("same_filtered_rows_eliminate_loose_aggregate",
         "SELECT strict.k, strict.total, loose.total + 1 FROM
          (SELECT k, sum(v) AS total FROM t WHERE v > 0 GROUP BY k HAVING sum(v) > 10) strict
          JOIN (SELECT k, sum(v) AS total FROM t WHERE v > 0 GROUP BY k) loose ON strict.k = loose.k", 1),
        ("different_rows_keep_both_aggregates",
         "SELECT strict.k, strict.total, loose.total + 1 FROM
          (SELECT k, sum(v) AS total FROM t WHERE v > 0 GROUP BY k HAVING sum(v) > 10) strict
          JOIN (SELECT k, sum(v) AS total FROM t GROUP BY k) loose ON strict.k = loose.k", 2),
    ] {
        let case = SqlTestCase {
            name,
            description: "Self-join elimination must consume logical aggregates before the late split boundary.",
            setup_sqls: &["CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)"],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let raw = ctx.bind_sql(case.sql).await?;
        let optimized = ctx.optimize_plan(raw.clone()).await?;
        let Plan::Query { s_expr, .. } = &optimized else {
            unreachable!()
        };
        struct Check {
            scans: usize,
        }
        impl ExprVisitor<Physical> for Check {
            fn visit(&mut self, expr: &PExpr) -> Result<PVisitAction> {
                self.scans += usize::from(expr.plan().as_scan().is_some());
                Ok(PVisitAction::Continue)
            }
        }
        let mut check = Check { scans: 0 };
        s_expr.planned()?.expr().accept(&mut check)?;
        assert_eq!(check.scans, expected_scans, "{name}");
        write_case_header(&mut file, &case)?;
        writeln!(
            file,
            "raw_plan:\n{}",
            raw.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        writeln!(
            file,
            "optimized_plan:\n{}",
            optimized.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
    }
    Ok(())
}

/// Default and Memo consumers share implementation definitions, including local
/// aggregate fragments nested under other aggregates. Record full plans from SQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_physical_implementation_consumers() -> Result<()> {
    use databend_common_sql::optimizer::ir::DefaultPhysicalPlanner;

    let mut file = open_golden_file("optimizer", "physical_implementation.txt")?;
    for (name, sql) in [
        ("grouped_aggregate", "SELECT k, sum(v) FROM t GROUP BY k"),
        (
            "nested_aggregate_fragments",
            "SELECT sum(total) FROM (SELECT k, sum(v) AS total FROM t GROUP BY k) grouped",
        ),
        (
            "union_aggregate_stage_dispatch",
            "SELECT k, sum(v) AS total FROM t WHERE v > 0 GROUP BY k
             UNION ALL SELECT k, sum(v) AS total FROM t WHERE v < 0 GROUP BY k
             ORDER BY k LIMIT 3",
        ),
        (
            "grouping_sets_stage_scope",
            "SELECT k, grouping(k) AS g, sum(v) AS total FROM t
             GROUP BY GROUPING SETS ((k), ())",
        ),
    ] {
        let case = SqlTestCase {
            name,
            description: "Default planning and Memo search consume the same local implementation definitions.",
            setup_sqls: &["CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)"],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let raw = ctx.bind_sql(sql).await?;
        let Plan::Query {
            s_expr, metadata, ..
        } = &raw
        else {
            unreachable!()
        };
        let default = DefaultPhysicalPlanner::plan(s_expr.logical()?)?;
        let opt_ctx =
            OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                .with_settings(&ctx.get_settings())?;
        let mut search = CascadesOptimizer::new(opt_ctx)?;
        let selected = search.optimize_sync(s_expr.logical()?.clone())?;
        assert!(
            search
                .memo()
                .root()
                .unwrap()
                .best_prop(&RequiredProperty::default())
                .is_some(),
            "Memo planning must not silently fall back"
        );
        for output in [&default, &selected] {
            output.validate_types(metadata)?;
            output.validate_column_scope(metadata)?;
            assert_eq!(
                output.derive_relational_prop()?.output_columns,
                s_expr.logical()?.derive_relational_prop()?.output_columns
            );
            assert!(output.derive_relational_prop()?.outer_columns.is_empty());
        }
        write_case_header(&mut file, &case)?;
        writeln!(
            file,
            "raw_plan:\n{}",
            raw.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        writeln!(
            file,
            "default_plan:\n{}",
            default.pretty_format(&metadata.read(), &ctx.stat_context()?)?
        )?;
        writeln!(
            file,
            "selected_plan:\n{}",
            selected.pretty_format(&metadata.read(), &ctx.stat_context()?)?
        )?;
    }
    Ok(())
}

/// Start from bound SQL directly so there are no preexisting split aggregates.
/// Inspect the memo as well as the output: fallback would otherwise hide a broken search.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cascades_implements_logical_aggregates() -> Result<()> {
    for sql in [
        "SELECT k, sum(v) FROM t GROUP BY k",
        "SELECT count(DISTINCT k) FROM t",
        "SELECT sum(total) FROM (SELECT k, sum(v) AS total FROM t GROUP BY k) grouped",
        "SELECT raw.k, sum(raw.v) + 1 FROM t raw JOIN t other ON raw.k = other.k GROUP BY raw.k",
    ] {
        for distributed in [false, true] {
            for cbo in [false, true] {
                let case = SqlTestCase {
                    name: "cascades_aggregate_implementation",
                    description: "Logical exploration precedes aggregate implementation, including local and fallback paths.",
                    setup_sqls: &["CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)"],
                    sql,
                };
                let ctx = setup_context(&case).await?;
                ctx.set_cluster_node_num(if distributed { 3 } else { 1 });
                ctx.get_settings().set_setting(
                    "enable_cbo".to_string(),
                    if cbo { "1" } else { "0" }.to_string(),
                )?;
                let Plan::Query {
                    s_expr, metadata, ..
                } = ctx.bind_sql(sql).await?
                else {
                    unreachable!()
                };
                let opt_ctx = OptimizerContext::new(
                    ctx.clone(),
                    metadata.clone(),
                    ctx.get_function_context()?,
                )
                .with_settings(&ctx.get_settings())?;
                opt_ctx.set_enable_distributed_optimization(distributed);
                let mut optimizer = CascadesOptimizer::new(opt_ctx)?;
                let output = optimizer.optimize_sync((*s_expr).into_logical()?)?;
                output.validate_types(&metadata)?;
                output.validate_column_scope(&metadata)?;
                let memo = optimizer.memo();
                let required = RequiredProperty {
                    distribution: if distributed {
                        Distribution::Serial
                    } else {
                        Distribution::Any
                    },
                };
                let best = memo.root().unwrap().best_prop(&required);
                if !cbo {
                    // CBO disabled reaches the existing task-limit fallback. Its output
                    // must still be split before heuristic distribution enforcement.
                    assert!(best.is_none());
                    continue;
                }
                let best = best.expect("CBO must produce a best plan, not use fallback");
                if distributed {
                    assert_eq!(best.physical_prop.distribution, Distribution::Serial);
                }
                let mut extractor = PatternExtractor::new();
                let mut logical = 0;
                let mut partial = 0;
                let mut final_ = 0;
                for index in 0..memo.num_groups() {
                    let group = memo.group(index)?;
                    let mut has_logical_aggregate = false;
                    let mut has_aggregate_implementation = false;
                    for expr_index in 0..group.num_exprs() {
                        let expr = group.m_expr(expr_index)?;
                        if let Some(plan) = expr.plan().logical() {
                            if plan.is_aggregate() {
                                logical += 1;
                                has_logical_aggregate = true;
                                assert!(
                                    !extractor
                                        .extract(memo, expr, &match_op!(Aggregate -> *))?
                                        .is_empty()
                                );
                            }
                            if let Some(best) = group.best_prop(&required) {
                                assert_ne!(
                                    best.expr_index, expr_index,
                                    "logical candidate was costed"
                                );
                            }
                        } else {
                            assert!(
                                extractor
                                    .extract(
                                        memo,
                                        expr,
                                        &databend_common_sql::optimizer::ir::Matcher::Leaf
                                    )?
                                    .is_empty(),
                                "physical candidate escaped into logical exploration"
                            );
                            if let Some(aggregate) = expr.plan().physical().unwrap().as_aggregate()
                            {
                                match aggregate.mode {
                                    AggregateMode::Partial => partial += 1,
                                    AggregateMode::Final => {
                                        final_ += 1;
                                        has_aggregate_implementation = true;
                                    }
                                }
                            }
                        }
                    }
                    assert!(
                        !has_logical_aggregate || has_aggregate_implementation,
                        "aggregate equivalence group has no physical implementation"
                    );
                }
                assert!(logical > 0 && partial > 0 && final_ > 0);
                if sql.contains("JOIN") {
                    // The bound query has one aggregate. More than one proves that
                    // Cascades explored and implemented eager alternatives as well.
                    assert!(logical > 1, "eager alternatives were not explored");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_memo_with_late_split() -> Result<()> {
    for sql in [
        "EXPLAIN MEMO SELECT raw.k, sum(raw.v) FROM t raw JOIN t other ON raw.k = other.k GROUP BY raw.k",
        "EXPLAIN MEMO SELECT count(DISTINCT k) FROM t",
    ] {
        let case = SqlTestCase {
            name: "memo_aggregate",
            description: "The separate EXPLAIN MEMO pipeline must also split after join optimization.",
            setup_sqls: &["CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)"],
            sql,
        };
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(sql).await?;
        let Plan::Explain { plan: query, .. } = &plan else {
            unreachable!()
        };
        let Plan::Query { metadata, .. } = query.as_ref() else {
            unreachable!()
        };
        let opt_ctx =
            OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                .with_settings(&ctx.get_settings())?;
        optimize(opt_ctx, plan).await?;
    }
    Ok(())
}
