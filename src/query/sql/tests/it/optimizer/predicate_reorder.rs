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
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimizers::DPhpyOptimizer;
use databend_common_sql::optimizer::optimizers::operator::PullUpFilterOptimizer;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use databend_common_sql::plans::Plan;

use super::column_stat;
use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

// These are characterization snapshots, not assertions that every retained
// predicate is necessary. In particular, a derived OR can become redundant
// after the join order changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_predicate_reorder_plans() -> Result<()> {
    let mut file = open_golden_file("optimizer", "predicate_reorder.txt")?;
    let cases = [
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
            name: "outer_join_boundary",
            description: "A non-null-rejecting residual must preserve the outer-join boundary.",
            setup_sqls: &[],
            sql: "SELECT a.k, c.v FROM a JOIN b ON a.k = b.k
LEFT JOIN c ON b.k = c.k
WHERE c.v IS NULL OR a.v = 1",
        },
    ];

    for case in &cases {
        write_case(&mut file, case).await?;
    }
    Ok(())
}

async fn write_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
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

    // Mirror the predicate-relevant stages preceding DPhyp. These queries have
    // no aggregate/CTE/subquery operators needing the other production stages.
    let raw = CollectStatisticsOptimizer::new(opt_ctx.clone())
        .optimize(*s_expr.clone())
        .await?;
    let before = PullUpFilterOptimizer::new(opt_ctx.clone()).optimize_sync(raw)?;
    let before = RecursiveRuleOptimizer::new(opt_ctx.clone(), &DEFAULT_REWRITE_RULES)
        .optimize_sync(before)?;
    let after = DPhpyOptimizer::new(opt_ctx.clone())
        .optimize_async(&before)
        .await?;

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
