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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimizers::DPhpyOptimizer;
use databend_common_sql::optimizer::optimizers::operator::PullUpFilterOptimizer;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;

use super::column_stat;
use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

fn has_null_safe_key(expr: &SExpr) -> bool {
    matches!(expr.plan(), RelOperator::Join(join) if join.has_null_equi_condition())
        || expr.children().any(has_null_safe_key)
}

// These are characterization snapshots, not assertions that every retained
// predicate is necessary. In particular, a derived OR can become redundant
// after the join order changes.
//
// Collision evidence (temporarily replacing JoinCondition insertion with
// unconditional writes, then running the same SQL-to-plan pipeline):
// - outer_to_inner_residual_collision retains the ON residual through pull-up
//   and writes the WHERE residual a second time when the join becomes inner;
// - nullable_lateral_correlation retains a NULL-safe key, skips equality
//   inference, and writes the existing ordinary key a second time;
// - ordinary inner-join controls do not change: pull-up first collects their
//   ON/WHERE predicates into one Filter, and rewrite removes exact duplicates.
// The position-only test below separately exercises DPhyp without pull-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_predicate_reorder_plans() -> Result<()> {
    let mut file = open_golden_file("optimizer", "dphyp_reorder.txt")?;
    let cases = [
        SqlTestCase {
            name: "hub_repeated_candidate_evaluation",
            description: "Independent bindings exercise repeated hub splits; reuse must preserve the complete plan and binding identities.",
            setup_sqls: &[],
            sql: "SELECT b0.k FROM b b0
JOIN b b1 ON b0.k = b1.k
JOIN b b2 ON b0.k = b2.k
JOIN b b3 ON b0.k = b3.k",
        },
        SqlTestCase {
            name: "single_table_or_extraction",
            description: "Single-table OR restrictions can reduce base inputs before reordering.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k
WHERE (a.v = 1 AND b.v = 10) OR (a.v = 2 AND b.v = 20)",
        },
        SqlTestCase {
            name: "multi_table_or_extraction",
            description: "Q13 shape: a necessary OR over a/b is useful below the join with c; inspect its position after reordering.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k JOIN c ON a.k = c.k
WHERE (a.v = 1 AND b.v > 10 AND c.v = 3)
   OR (a.v = 2 AND b.v < 20 AND c.v = 1)
   OR (a.v = 3 AND b.v = 30 AND c.v = 1)",
        },
        SqlTestCase {
            name: "multi_table_or_alternate_input_order",
            description: "The same filtering semantics start from a different join cut.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN c ON a.k = c.k JOIN b ON a.k = b.k
WHERE (a.v = 1 AND b.v > 10 AND c.v = 3)
   OR (a.v = 2 AND b.v < 20 AND c.v = 1)
   OR (a.v = 3 AND b.v = 30 AND c.v = 1)",
        },
        SqlTestCase {
            name: "two_relation_residual",
            description: "A required non-equality condition must survive collection and reconstruction.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k
WHERE a.v + b.v > 50",
        },
        SqlTestCase {
            name: "complex_residual_on_cross_join",
            description: "A complex cross-side Other predicate becomes a non-equi condition and turns CROSS into INNER.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a CROSS JOIN b WHERE a.v + b.v > 50",
        },
        SqlTestCase {
            name: "cross_side_or_residual",
            description: "An OR whose branches each use both inputs is a join residual, not separable left/right operands.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k WHERE (a.v = 1 AND b.v > 10) OR (a.v = 2 AND b.v < 20)",
        },
        SqlTestCase {
            name: "volatile_cross_side_other",
            description: "A volatile Other predicate stays above the join instead of changing evaluation location.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k WHERE rand() + a.v + b.v > 50",
        },
        SqlTestCase {
            name: "outer_join_complex_other",
            description: "A non-null-rejecting complex Other predicate cannot move from WHERE into a LEFT JOIN ON clause.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a LEFT JOIN b ON a.k = b.k WHERE b.v IS NULL OR a.v + b.v > 50",
        },
        SqlTestCase {
            name: "three_relation_residual",
            description: "A predicate requiring all three inputs cannot be applied to a smaller subtree.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k
WHERE a.v + b.v > c.v",
        },
        SqlTestCase {
            name: "equality_chain",
            description: "Record the equality conditions chosen from a transitive chain.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k",
        },
        SqlTestCase {
            name: "equality_constant_propagation",
            description: "A constant propagated across equality can change the inputs seen by reorder.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k
WHERE a.k = 7",
        },
        SqlTestCase {
            name: "both_sides_already_filtered",
            description: "Both sides already contain the predicate that equality substitution can derive.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k
WHERE a.k % 2 = 0 AND b.k % 2 = 0",
        },
        SqlTestCase {
            name: "alias_rewrite_collision",
            description: "Distinct projected aliases become the same scalar after filter pushdown.",
            setup_sqls: &[],
            sql: "SELECT x FROM (SELECT k AS x, k AS y FROM a) t
WHERE x > 10 AND y > 10",
        },
        SqlTestCase {
            name: "on_where_non_equi_collision",
            description: "Control: pull-up and rewrite already merge the repeated inner ON/WHERE residual before placement.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > b.v WHERE a.v > b.v",
        },
        SqlTestCase {
            name: "outer_to_inner_residual_collision",
            description: "A null-rejecting WHERE turns an outer join into inner; unlike the inner input, its ON residual was not pulled up first.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a LEFT JOIN b ON a.k = b.k AND a.v > b.v WHERE a.v > b.v",
        },
        SqlTestCase {
            name: "on_where_equi_collision",
            description: "A repeated ordinary equality is a control for the normal equality-inference path.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.k = b.k",
        },
        SqlTestCase {
            name: "residuals_from_different_join_scopes",
            description: "Control: full heuristic rewriting merges equal residuals from different inner joins before DPhyp.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > b.v JOIN c ON b.k = c.k AND a.v > b.v",
        },
        SqlTestCase {
            name: "repeated_residual_in_one_on_clause",
            description: "Control: full heuristic rewriting removes repeated inner ON residuals before placement.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > b.v AND a.v > b.v",
        },
        SqlTestCase {
            name: "distinct_residuals_same_inputs",
            description: "Two different residuals over the same inputs must both survive placement.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > b.v WHERE a.v + b.v > 50",
        },
        SqlTestCase {
            name: "null_safe_comparison_and_ordinary_equality",
            description: "IS NOT DISTINCT FROM is expanded by the binder; it is not evidence of a NULL-safe join key.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v IS NOT DISTINCT FROM b.v WHERE a.k = b.k",
        },
        SqlTestCase {
            name: "nullable_lateral_correlation",
            description: "A NULL-safe correlation key skips equality inference; inserting the repeated ordinary key must not duplicate it.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a LEFT JOIN b ON a.k = b.k JOIN LATERAL (SELECT c.k FROM c WHERE c.v = b.v GROUP BY c.k) t ON a.k = t.k WHERE a.k = t.k",
        },
        SqlTestCase {
            name: "outer_join_cross_side_residual",
            description: "A cross-side outer ON residual stays inside the outer join after heuristic rewrites and DPhyp.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k LEFT JOIN c ON b.k = c.k AND c.v > a.v",
        },
        SqlTestCase {
            name: "outer_join_boundary",
            description: "A non-null-rejecting residual must preserve the outer-join boundary.",
            setup_sqls: &[],
            sql: "SELECT a.k, c.v FROM a JOIN b ON a.k = b.k
LEFT JOIN c ON b.k = c.k
WHERE c.v IS NULL OR a.v = 1",
        },
    ];

    for case in &cases {
        write_case(&mut file, case, true).await?;
    }
    Ok(())
}

// Only remove identity projections before DPhyp, so it sees the original
// scopes and places existing predicates without a second inference pass.
// residual_collision_without_pull_up produces two identical join residuals
// with unconditional writes, unlike the full-pipeline control above.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dphyp_filter_positions() -> Result<()> {
    let mut file = open_golden_file("optimizer", "dphyp_filter_position.txt")?;
    let cases = [
        SqlTestCase {
            name: "filter_scope_exceeds_used_columns",
            description: "Use the original a/b scope to find the single-table provider and place the predicate directly on a.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.v > 10) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "selective_filter_keeps_covered_subtree",
            description: "Place the single-table filter before evaluating candidates so its selectivity participates in join-order choice.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.v = 1) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "same_scalar_different_scopes",
            description: "A collected filter and an unchanged root filter can have the same scalar; preserve both placement contexts.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.k > 10) t JOIN c ON t.k = c.k WHERE t.k > 10",
        },
        SqlTestCase {
            name: "two_scoped_filters",
            description: "Place the two-table residual where both inputs are available and preserve the unchanged root filter.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.v + b.v > 10) t JOIN c ON t.k = c.k WHERE c.v > 1",
        },
        SqlTestCase {
            name: "inner_join_on_residual",
            description: "Place a non-equality ON condition where its actual inputs first become available within the original scope.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > b.v JOIN c ON b.k = c.k",
        },
        SqlTestCase {
            name: "complex_other_without_rewrite",
            description: "DPhyp directly places an existing complex Other filter as a residual at the first join providing both inputs.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.v + b.v > 50) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "cross_side_or_without_rewrite",
            description: "DPhyp places an existing OR as one intact residual, without OR extraction or expression splitting.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE (a.v = 1 AND b.v > 10) OR (a.v = 2 AND b.v < 20)) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "split_filter_by_required_relations",
            description: "One original Filter contains predicates needing different inputs; place them independently without generating new predicates.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE a.v = 1 AND b.v > 10 AND a.v + b.v > 20) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "residual_collision_without_pull_up",
            description: "Bypass pull-up to verify that identical ON residuals from different scopes collide during DPhyp candidate placement.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > b.v JOIN c ON b.k = c.k AND a.v > b.v",
        },
        SqlTestCase {
            name: "single_side_on_residual",
            description: "A deterministic single-side inner ON residual becomes a base filter instead of staying on a larger join.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k AND a.v > 10 JOIN c ON b.k = c.k",
        },
        SqlTestCase {
            name: "constant_keeps_original_scope",
            description: "A no-column predicate retains the collected a/b scope and is neither copied to both inputs nor inferred.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE false) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "volatile_keeps_original_scope",
            description: "A volatile filter with a referenced column must not be narrowed to that column's base relation.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a JOIN b ON a.k = b.k WHERE rand() + a.v > 10) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "outer_join_on_residual",
            description: "An outer join ON residual must remain in the opaque outer join, not become a WHERE filter.",
            setup_sqls: &[],
            sql: "SELECT a.k FROM a JOIN b ON a.k = b.k LEFT JOIN c ON b.k = c.k AND c.v > a.v",
        },
        SqlTestCase {
            name: "filter_above_outer_join",
            description: "Retain a filter above an opaque outer-join relation when surrounding inner joins reorder.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT a.k FROM a LEFT JOIN b ON a.k = b.k AND b.v > 10 WHERE b.v IS NULL) t JOIN c ON t.k = c.k",
        },
        SqlTestCase {
            name: "filter_above_limit",
            description: "Keep the filter above the LIMIT boundary; do not reconstruct it below the limit.",
            setup_sqls: &[],
            sql: "SELECT t.k FROM (SELECT k, v FROM a LIMIT 10) t JOIN b ON t.k = b.k JOIN c ON b.k = c.k WHERE t.v > 1",
        },
    ];
    for case in &cases {
        write_case(&mut file, case, false).await?;
    }
    Ok(())
}

async fn write_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    rewrite: bool,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    // Explicit statistics avoid empty-table plans and make the choice between
    // different cuts observable without a service or a TPC-DS data load.
    for (name, rows) in [("a", 10_000), ("b", 1_000), ("c", 100)] {
        let stats =
            column_stat(r#"{"min":0,"max":99,"ndv":100,"null_count":0,"in_memory_size":800}"#)?;
        ctx.register_table_sql_with_stats(
            &format!("CREATE TABLE {name}(k BIGINT NOT NULL, v BIGINT NOT NULL)"),
            Some(table_statistics(rows)),
            HashMap::from([("k".to_string(), stats.clone()), ("v".to_string(), stats)]),
            HashMap::new(),
        )
        .await?;
    }
    for (name, value) in [("enable_dphyp", "1"), ("disable_join_reorder", "0")] {
        ctx.get_settings()
            .set_setting(name.to_string(), value.to_string())?;
    }

    let plan = ctx.bind_sql(case.sql).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = &plan
    else {
        unreachable!("test query should bind to Plan::Query")
    };
    let settings = ctx.get_settings();
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
        .with_settings(&settings)?;

    // Mirror the predicate-relevant stages preceding DPhyp. The lateral case
    // is flattened by binding; this harness does not run the full optimizer.
    let raw = CollectStatisticsOptimizer::new(opt_ctx.clone())
        .optimize(*s_expr.clone())
        .await?;
    if case.name == "nullable_lateral_correlation" {
        assert!(
            has_null_safe_key(&raw),
            "binding must produce a NULL-safe correlation key"
        );
    }
    let before = if rewrite {
        let expr = PullUpFilterOptimizer::new(opt_ctx.clone()).optimize_sync(raw)?;
        RecursiveRuleOptimizer::new(opt_ctx.clone(), &DEFAULT_REWRITE_RULES).optimize_sync(expr)?
    } else {
        RecursiveRuleOptimizer::new(opt_ctx.clone(), &[RuleID::EliminateEvalScalar])
            .optimize_sync(raw)?
    };
    if case.name == "nullable_lateral_correlation" {
        assert!(
            has_null_safe_key(&before),
            "heuristic rewriting must preserve the NULL-safe key"
        );
    }
    let after = DPhpyOptimizer::new(opt_ctx.clone())
        .optimize_async(&before)
        .await?;

    if case.name == "nullable_lateral_correlation" {
        assert!(
            has_null_safe_key(&after),
            "reordering must preserve the NULL-safe key"
        );
    }
    write_case_header(file, case)?;
    writeln!(file, "settings: enable_dphyp=1, disable_join_reorder=0")?;
    after.validate_types(metadata)?;
    after.validate_column_scope(metadata)?;
    writeln!(file, "plan:")?;
    writeln!(
        file,
        "{}",
        plan.replace_query_s_expr(after)
            .format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file)?;
    Ok(())
}
