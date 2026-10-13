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
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::QueryPlan;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimize;
use databend_common_sql::plans::Plan;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_planning_boundary() -> Result<()> {
    let mut file = open_golden_file("optimizer", "physical_planner.txt")?;
    for distributed in [false, true] {
        for cbo in [false, true] {
            let ctx = LiteTableContext::create().await?;
            ctx.set_cluster_node_num(if distributed { 2 } else { 1 });
            ctx.set_table_warehouse_distribution(distributed);
            ctx.register_setup_sql("CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)")
                .await?;
            ctx.get_settings()
                .set_setting("enable_cbo".to_string(), u8::from(cbo).to_string())?;
            ctx.get_settings()
                .set_setting("enable_optimizer_trace".to_string(), "1".to_string())?;
            let raw = ctx
                .bind_sql("SELECT k, sum(v) FROM t GROUP BY k ORDER BY k LIMIT 5")
                .await?;
            let Plan::Query {
                s_expr, metadata, ..
            } = &raw
            else {
                unreachable!()
            };
            assert!(matches!(s_expr.as_ref(), QueryPlan::Logical(_)));
            assert!(
                s_expr.planned().is_err(),
                "bound input must not reach execution"
            );
            let context =
                OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                    .with_settings(&ctx.get_settings())?;
            context.set_enable_distributed_optimization(distributed);
            let planned = optimize(context, raw.clone()).await?;
            let Plan::Query { s_expr, .. } = &planned else {
                unreachable!()
            };
            assert!(matches!(s_expr.as_ref(), QueryPlan::Planned(_)));
            assert!(
                (*s_expr.clone()).into_logical().is_err(),
                "selected implementations are not logical inputs"
            );
            let implementation = s_expr.planned()?;
            if distributed {
                assert!(implementation.expr().has_merge_exchange());
            }
            implementation.expr().validate_types(metadata)?;
            implementation.expr().validate_column_scope(metadata)?;
            assert_eq!(raw.schema(), planned.schema());
            writeln!(
                file,
                "=== query_boundary: distributed={distributed}, cbo={cbo} ==="
            )?;
            writeln!(
                file,
                "sql: SELECT k, sum(v) FROM t GROUP BY k ORDER BY k LIMIT 5"
            )?;
            writeln!(
                file,
                "raw_plan:\n{}",
                raw.format_indent(Default::default(), &StatContext::default())?
            )?;
            writeln!(
                file,
                "planned_query:\n{}",
                planned.format_indent(Default::default(), &StatContext::default())?
            )?;
            let without_merge = planned.remove_exchange_for_select();
            let Plan::Query { s_expr, .. } = without_merge else {
                unreachable!()
            };
            assert!(
                s_expr.planned().is_ok(),
                "consumer adapter must preserve planned state"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cascades_logical_input_physical_output() -> Result<()> {
    use databend_common_sql::optimizer::ir::Distribution;
    use databend_common_sql::optimizer::ir::PExpr;
    use databend_common_sql::optimizer::ir::RequiredProperty;
    use databend_common_sql::optimizer::optimizers::CascadesOptimizer;

    for cbo in [false, true] {
        let ctx = LiteTableContext::create().await?;
        ctx.register_setup_sql("CREATE TABLE t(k Int64 NOT NULL)")
            .await?;
        ctx.get_settings()
            .set_setting("enable_cbo".to_string(), u8::from(cbo).to_string())?;
        let Plan::Query {
            s_expr, metadata, ..
        } = ctx.bind_sql("SELECT k FROM t").await?
        else {
            unreachable!()
        };
        let context =
            OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                .with_settings(&ctx.get_settings())?;
        let mut search = CascadesOptimizer::new(context)?;
        let output: PExpr = search.optimize_sync((*s_expr).into_logical()?)?;
        output.validate_types(&metadata)?;
        output.validate_column_scope(&metadata)?;
        let root = search
            .memo()
            .root()
            .expect("search initializes the memo even on fallback");
        assert_eq!(
            root.best_prop(&RequiredProperty {
                distribution: Distribution::Any
            })
            .is_some(),
            cbo
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_physical_planner_explain_and_skip_compatibility() -> Result<()> {
    for skipped in ["", "CascadesOptimizer"] {
        for sql in [
            "SELECT sum(v) FROM t",
            "EXPLAIN MEMO SELECT sum(v) FROM t",
            "EXPLAIN DECORRELATED SELECT k FROM t",
        ] {
            let ctx = LiteTableContext::create().await?;
            ctx.register_setup_sql("CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)")
                .await?;
            ctx.get_settings()
                .set_optimizer_skip_list(skipped.to_string())?;
            let raw = ctx.bind_sql(sql).await?;
            let metadata = match &raw {
                Plan::Query { metadata, .. } => metadata.clone(),
                Plan::Explain { plan, .. } => match plan.as_ref() {
                    Plan::Query { metadata, .. } => metadata.clone(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };
            let context = OptimizerContext::new(ctx.clone(), metadata, ctx.get_function_context()?)
                .with_settings(&ctx.get_settings())?;
            let planned = optimize(context, raw).await?;
            match planned {
                Plan::Query { s_expr, .. } => {
                    s_expr.planned()?;
                }
                Plan::Explain { plan, .. } => {
                    let Plan::Query { s_expr, .. } = plan.as_ref() else {
                        unreachable!()
                    };
                    // MEMO displays the search result, DECORRELATED stops before selection.
                    assert!(matches!(s_expr.as_ref(), QueryPlan::Logical(_)));
                }
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_nested_queries_keep_planned_state() -> Result<()> {
    let mut file = open_golden_file("optimizer", "physical_planner_nested.txt")?;
    for sql in [
        "INSERT INTO dst SELECT k, v FROM t",
        "REPLACE INTO dst ON(k) SELECT k, v FROM t",
        "CREATE TABLE copied ENGINE=NULL AS SELECT k, v FROM t",
        "INSERT ALL WHEN k IN (SELECT k FROM t) THEN INTO dst VALUES(k, v) ELSE INTO dst2 VALUES(k, v) SELECT k, v FROM t",
    ] {
        let ctx = LiteTableContext::create().await?;
        for setup in [
            "CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)",
            "CREATE TABLE dst(k Int64 NOT NULL, v Int64 NOT NULL)",
            "CREATE TABLE dst2(k Int64 NOT NULL, v Int64 NOT NULL)",
        ] {
            ctx.register_setup_sql(setup).await?;
        }
        let raw = ctx.bind_sql(sql).await?;
        fn source(plan: &Plan) -> &Plan {
            match plan {
                Plan::Insert(plan) => match &plan.source {
                    databend_common_sql::InsertInputSource::SelectPlan(query) => query,
                    _ => unreachable!(),
                },
                Plan::Replace(plan) => match &plan.source {
                    databend_common_sql::InsertInputSource::SelectPlan(query) => query,
                    _ => unreachable!(),
                },
                Plan::CreateTable(plan) => plan.as_select.as_deref().unwrap(),
                Plan::InsertMultiTable(plan) => &plan.input_source,
                _ => unreachable!(),
            }
        }
        let Plan::Query { metadata, .. } = source(&raw) else {
            unreachable!()
        };
        let context =
            OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                .with_settings(&ctx.get_settings())?;
        let planned = optimize(context, raw.clone()).await?;
        let Plan::Query { s_expr, .. } = source(&planned) else {
            unreachable!()
        };
        s_expr.planned()?.expr().validate_types(metadata)?;
        s_expr.planned()?.expr().validate_column_scope(metadata)?;
        writeln!(file, "sql: {sql}")?;
        writeln!(
            file,
            "raw_source:\n{}",
            source(&raw).format_indent(Default::default(), &StatContext::default())?
        )?;
        writeln!(
            file,
            "planned_source:\n{}",
            source(&planned).format_indent(Default::default(), &StatContext::default())?
        )?;
    }
    Ok(())
}

/// SQL produces the trees under test; compare both representations before allowing
/// their implementations to diverge. Test forward conversion and invalidation.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_physical_expression_fork_equivalence() -> Result<()> {
    use std::hash::Hash;
    use std::hash::Hasher;

    use databend_common_sql::optimizer::ir::ExprVisitor;
    use databend_common_sql::optimizer::ir::PExpr;
    use databend_common_sql::optimizer::ir::PVisitAction as VisitAction;
    use databend_common_sql::optimizer::ir::RelExpr;

    for sql in [
        "SELECT k, sum(v) FROM t GROUP BY k ORDER BY k LIMIT 5",
        "WITH c AS (SELECT k, v FROM t) SELECT a.k FROM c a JOIN c b ON a.k = b.k",
        "SELECT a.k FROM t a LEFT JOIN t b ON a.k = b.k WHERE b.v > 0",
    ] {
        let ctx = LiteTableContext::create().await?;
        ctx.register_setup_sql("CREATE TABLE t(k Int64 NOT NULL, v Int64 NOT NULL)")
            .await?;
        let raw = ctx.bind_sql(sql).await?;
        let Plan::Query {
            s_expr, metadata, ..
        } = &raw
        else {
            unreachable!()
        };
        let logical = s_expr.logical()?.clone();
        let property = logical.derive_relational_prop()?;
        let statistics =
            RelExpr::with_s_expr(&logical).derive_cardinality(&StatContext::default())?;
        let physical = PExpr::from(logical.clone());
        assert!(std::sync::Arc::ptr_eq(
            &property,
            &physical.derive_relational_prop()?
        ));
        assert!(std::sync::Arc::ptr_eq(
            &statistics,
            &RelExpr::with_p_expr(&physical).derive_cardinality(&StatContext::default())?
        ));
        physical.validate_types(metadata)?;
        physical.validate_column_scope(metadata)?;
        assert_eq!(
            logical.pretty_format(&metadata.read(), &StatContext::default())?,
            physical.pretty_format(&metadata.read(), &StatContext::default())?
        );
        fn hash(expr: &impl Hash) -> u64 {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            expr.hash(&mut h);
            h.finish()
        }
        assert_eq!(hash(&logical), hash(&physical));
        let replaced = physical.replace_plan(physical.plan.clone());
        // Some operators return their child's Arc directly. Compare replacement
        // behavior rather than assuming a newly allocated property value.
        assert_eq!(
            format!(
                "{:?}",
                logical
                    .replace_plan(logical.plan.clone())
                    .derive_relational_prop()?
            ),
            format!("{:?}", replaced.derive_relational_prop()?)
        );
        assert_eq!(physical, replaced);
        struct Count(usize);
        impl ExprVisitor<databend_common_sql::optimizer::ir::Physical> for Count {
            fn visit(&mut self, _: &PExpr) -> Result<VisitAction> {
                self.0 += 1;
                Ok(VisitAction::Continue)
            }
        }
        let mut count = Count(0);
        assert!(physical.accept(&mut count)?.is_none());
        assert!(count.0 > 1);
        let planned = ctx.optimize_plan(raw).await?;
        let Plan::Query {
            s_expr, metadata, ..
        } = &planned
        else {
            unreachable!()
        };
        let selected = s_expr.planned()?.expr();
        selected.validate_types(metadata)?;
        selected.validate_column_scope(metadata)?;
        // A selected implementation is inspected directly, never reconstructed as
        // a logical tree merely to exercise the old property interface.
        RelExpr::with_p_expr(selected).derive_physical_prop()?;
    }
    Ok(())
}
