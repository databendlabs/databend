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
use databend_common_sql::optimizer::optimize;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::Plan;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_when_subqueries_are_planned_with_source() -> Result<()> {
    let mut file = open_golden_file("optimizer", "insert_multi_table.txt")?;
    for (name, sql, has_join) in [
        (
            "no_subquery",
            "INSERT ALL WHEN k > 0 THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t",
            false,
        ),
        (
            "constant_subquery",
            "INSERT ALL WHEN k = (SELECT 1) THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t",
            false,
        ),
        (
            "in_subquery",
            "INSERT ALL WHEN k IN (SELECT k FROM lookup) THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t",
            true,
        ),
        (
            "correlated_exists",
            "INSERT ALL WHEN EXISTS (SELECT 1 FROM lookup WHERE lookup.k = s.k) THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t s",
            true,
        ),
        (
            "scalar_aggregate",
            "INSERT ALL WHEN k > (SELECT max(k) FROM lookup) THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t",
            true,
        ),
        (
            "multiple_first",
            "INSERT FIRST WHEN k IN (SELECT k FROM lookup) THEN INTO dst WHEN k > (SELECT min(k) FROM lookup) THEN INTO dst2 ELSE INTO dst SELECT k, v FROM t",
            true,
        ),
        (
            "nullable_not_in",
            "INSERT ALL WHEN k NOT IN (SELECT k FROM lookup) THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t",
            true,
        ),
        (
            "source_limit",
            "INSERT ALL WHEN k IN (SELECT k FROM lookup) THEN INTO dst ELSE INTO dst2 SELECT k, v FROM t ORDER BY k LIMIT 2",
            true,
        ),
    ] {
        for cbo in [false, true] {
            for distributed in [false, true] {
                let case = SqlTestCase {
                    name,
                    description: "WHEN decorrelation introduces logical nodes before source planning; branch symbols and source column order survive optimization.",
                    setup_sqls: &[
                        "CREATE TABLE t(k Int64 NULL, v Int64 NULL)",
                        "CREATE TABLE lookup(k Int64 NULL)",
                        "CREATE TABLE dst(k Int64 NULL, v Int64 NULL)",
                        "CREATE TABLE dst2(k Int64 NULL, v Int64 NULL)",
                    ],
                    sql,
                };
                let ctx = setup_context(&case).await?;
                ctx.set_cluster_node_num(if distributed { 2 } else { 1 });
                ctx.get_settings()
                    .set_setting("enable_cbo".to_string(), u8::from(cbo).to_string())?;
                let raw = ctx.bind_sql(sql).await?;
                let Plan::InsertMultiTable(bound) = &raw else {
                    unreachable!()
                };
                let Plan::Query { bind_context, .. } = &bound.input_source else {
                    unreachable!()
                };
                let source_columns = bind_context.result_columns();
                let context = OptimizerContext::new(
                    ctx.clone(),
                    bound.meta_data.clone(),
                    ctx.get_function_context()?,
                )
                .with_settings(&ctx.get_settings())?;
                context.set_enable_distributed_optimization(distributed);
                let planned = optimize(context, raw.clone()).await?;
                let Plan::InsertMultiTable(insert) = &planned else {
                    unreachable!()
                };
                let Plan::Query {
                    s_expr,
                    bind_context,
                    ..
                } = &insert.input_source
                else {
                    unreachable!()
                };
                assert_eq!(source_columns, bind_context.result_columns());
                let source = s_expr.planned()?.expr();
                source.validate_types(&insert.meta_data)?;
                source.validate_column_scope(&insert.meta_data)?;
                let property = source.derive_relational_prop()?;
                let output = &property.output_columns;
                for when in &insert.whens {
                    assert!(!when.condition.has_subquery());
                    assert!(
                        when.condition
                            .used_columns()
                            .iter()
                            .all(|column| output.contains(column))
                    );
                }
                struct Check {
                    joins: usize,
                }
                impl ExprVisitor<databend_common_sql::optimizer::ir::Physical> for Check {
                    fn visit(&mut self, expr: &PExpr) -> Result<VisitAction> {
                        self.joins += usize::from(expr.plan().as_join().is_some());
                        if let Some(aggregate) = expr.plan().as_aggregate() {
                            assert_ne!(
                                aggregate.mode,
                                AggregateMode::Initial,
                                "WHEN aggregate skipped source lowering"
                            );
                        }
                        Ok(VisitAction::Continue)
                    }
                }
                let mut check = Check { joins: 0 };
                source.accept(&mut check)?;
                assert_eq!(check.joins > 0, has_join, "{name}");
                if !cbo && !distributed {
                    write_case_header(&mut file, &case)?;
                    writeln!(
                        file,
                        "raw_source:\n{}",
                        bound
                            .input_source
                            .format_indent(Default::default(), &ctx.stat_context()?)?
                    )?;
                }
                writeln!(file, "cbo: {cbo}, distributed: {distributed}")?;
                writeln!(
                    file,
                    "planned_source:\n{}",
                    insert
                        .input_source
                        .format_indent(Default::default(), &ctx.stat_context()?)?
                )?;
                for (index, when) in insert.whens.iter().enumerate() {
                    writeln!(
                        file,
                        "when_{index}: {}",
                        when.condition.as_expr()?.sql_display()
                    )?;
                }
            }
        }
    }
    Ok(())
}
