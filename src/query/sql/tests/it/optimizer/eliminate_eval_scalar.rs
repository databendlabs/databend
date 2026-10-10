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
use databend_common_sql::optimizer::ir::ExprVisitor;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::PVisitAction as VisitAction;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::optimize;
use databend_common_sql::optimizer::optimizers::operator::EliminateEvalScalarOptimizer;
use databend_common_sql::plans::Plan;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

fn eval_count(expr: &PExpr) -> Result<usize> {
    struct Count(usize);
    impl ExprVisitor<databend_common_sql::optimizer::ir::Physical> for Count {
        fn visit(&mut self, expr: &PExpr) -> Result<VisitAction> {
            self.0 += usize::from(expr.plan().as_eval_scalar().is_some());
            Ok(VisitAction::Continue)
        }
    }
    let mut count = Count(0);
    expr.accept(&mut count)?;
    Ok(count.0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_physical_eliminate_eval_scalar() -> Result<()> {
    let mut file = open_golden_file("optimizer", "eliminate_eval_scalar.txt")?;
    for (name, sql, expected_evals) in [
        ("identity_projection", "SELECT k FROM t", 0),
        ("computed_projection", "SELECT k + 1 FROM t", 1),
        ("constant_projection", "SELECT 42 FROM t", 1),
        ("cast_projection", "SELECT CAST(k AS String) FROM t", 1),
        (
            "nested_identity",
            "SELECT x FROM (SELECT k + 1 AS x FROM t) q",
            1,
        ),
    ] {
        let skips: &[&str] = if expected_evals == 0 {
            &[
                "",
                "EliminateEvalScalar",
                "RecursiveRuleOptimizer[EliminateEvalScalar]",
            ]
        } else {
            &[""]
        };
        for &skipped in skips {
            let case = SqlTestCase {
                name,
                description: "Physical EvalScalar cleanup removes identity projections but keeps computations, and honors both skip levels.",
                setup_sqls: &["CREATE TABLE t(k Int64 NOT NULL)"],
                sql,
            };
            let ctx = setup_context(&case).await?;
            ctx.get_settings()
                .set_optimizer_skip_list(skipped.to_string())?;
            ctx.get_settings()
                .set_setting("enable_optimizer_trace".to_string(), "1".to_string())?;
            let raw = ctx.bind_sql(sql).await?;
            let Plan::Query { metadata, .. } = &raw else {
                unreachable!()
            };
            let context =
                OptimizerContext::new(ctx.clone(), metadata.clone(), ctx.get_function_context()?)
                    .with_settings(&ctx.get_settings())?;
            let planned = optimize(context.clone(), raw.clone()).await?;
            let Plan::Query { s_expr, .. } = &planned else {
                unreachable!()
            };
            let expr = s_expr.planned()?.expr();
            expr.validate_types(metadata)?;
            expr.validate_column_scope(metadata)?;
            assert_eq!(raw.schema(), planned.schema());
            if skipped.is_empty() {
                assert_eq!(eval_count(expr)?, expected_evals, "{name}");
            } else {
                // Rule-level skip remains in the cleanup entry; whole-pass skip is
                // enforced by PhysicalPlanner. Directly run the same physical cleanup
                // with an enabled context to prove the skipped node is reachable.
                ctx.get_settings().set_optimizer_skip_list(String::new())?;
                let enabled = OptimizerContext::new(
                    ctx.clone(),
                    metadata.clone(),
                    ctx.get_function_context()?,
                )
                .with_settings(&ctx.get_settings())?;
                let cleaned =
                    EliminateEvalScalarOptimizer::new(enabled).optimize_sync(expr.clone())?;
                assert_eq!(eval_count(&cleaned)?, expected_evals, "{name}");
                assert!(
                    eval_count(expr)? > 0,
                    "skip did not retain identity projection"
                );
            }
            if skipped.is_empty() {
                write_case_header(&mut file, &case)?;
                writeln!(
                    file,
                    "raw_plan:\n{}",
                    raw.format_indent(Default::default(), &StatContext::default())?
                )?;
            }
            writeln!(file, "skip: {skipped}")?;
            writeln!(
                file,
                "optimized_plan:\n{}",
                planned.format_indent(Default::default(), &StatContext::default())?
            )?;
        }
    }
    Ok(())
}
