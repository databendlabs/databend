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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::setup_context;

const TABLE: &str = "CREATE TABLE scope_t(a Int64, b Int64, c Int64)";

/// Drop the given column from every `Scan` in the tree, leaving the operators above
/// referencing a column that nothing produces. This is the shape a binder or optimizer
/// bug leaves behind (e.g. a decorrelation that forgot a column, or a pruning rule that
/// removed a column still needed above); at execution time it surfaces as an opaque
/// `Unable to get field named "<symbol>"`.
fn drop_scan_column(
    s_expr: &SExpr,
    name: &str,
    metadata: &databend_common_sql::MetadataRef,
) -> SExpr {
    let children = s_expr
        .children()
        .map(|child| Arc::new(drop_scan_column(child, name, metadata)))
        .collect::<Vec<_>>();
    let s_expr = s_expr.replace_children(children);
    let RelOperator::Scan(scan) = s_expr.plan() else {
        return s_expr;
    };
    let mut scan = scan.clone();
    scan.columns
        .retain(|column| metadata.read().column(*column).name() != name);
    s_expr.replace_plan(Arc::new(RelOperator::Scan(scan)))
}

// The scope check runs under `debug_assertions`, like the type check next to it.
#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_optimizer_rejects_plan_referencing_missing_column() -> Result<()> {
    let case = SqlTestCase {
        name: "filter_references_column_dropped_from_scan",
        description: "",
        setup_sqls: &[TABLE],
        sql: "SELECT a FROM scope_t WHERE b > 1",
    };
    let ctx = setup_context(&case).await?;
    let plan = ctx.bind_sql(case.sql).await?;

    // The bound plan is well-formed.
    ctx.optimize_plan(plan.clone()).await?;

    let Plan::Query {
        s_expr,
        metadata,
        bind_context,
        rewrite_kind,
        formatted_ast,
        ignore_result,
    } = plan
    else {
        unreachable!("expected a query plan");
    };
    let broken = Plan::Query {
        s_expr: Box::new(drop_scan_column(&s_expr, "b", &metadata)),
        metadata,
        bind_context,
        rewrite_kind,
        formatted_ast,
        ignore_result,
    };

    let err = ctx.optimize_plan(broken).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::INTERNAL, "{err}");
    assert!(
        err.message().contains("column scope violation in Filter") && err.message().contains("(b)"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_optimizer_accepts_correlated_and_lateral_scopes() -> Result<()> {
    // Correlated subqueries and LATERAL joins legitimately reference columns of the
    // enclosing scope; the validator must resolve them through that scope rather than
    // through the operator's own children.
    let cases = [
        SqlTestCase {
            name: "correlated_exists_with_join_on",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT a FROM scope_t t1 WHERE EXISTS (
                SELECT 1 FROM scope_t t2 JOIN scope_t t3 ON t2.a = t1.a AND t2.b = t3.b)",
        },
        SqlTestCase {
            name: "lateral_values",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t1.a, v.c1 FROM scope_t t1
                LEFT JOIN LATERAL (VALUES ('b', t1.b), ('c', t1.c)) AS v(tag, c1) ON t1.a = v.c1",
        },
        SqlTestCase {
            name: "scalar_subquery_in_select",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT a, (SELECT max(b) FROM scope_t t2 WHERE t2.c = t1.c) FROM scope_t t1",
        },
        SqlTestCase {
            name: "left_semi_retained_column",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t1.a FROM scope_t t1 LEFT SEMI JOIN scope_t t2 ON t1.a = t2.a
                WHERE t1.b > 1",
        },
        SqlTestCase {
            name: "right_anti_retained_column",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t2.a FROM scope_t t1 RIGHT ANTI JOIN scope_t t2 ON t1.a = t2.a
                WHERE t2.b > 1",
        },
        SqlTestCase {
            name: "window_group_with_scalar_items",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT a, row_number() OVER (PARTITION BY b ORDER BY c) FROM scope_t",
        },
    ];
    for case in cases {
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        ctx.optimize_plan(plan).await?;
    }
    Ok(())
}

/// Collects `EvalScalar` items that project a column their own child does not produce,
/// and counts the expression scans so a test can prove it exercised the lateral `VALUES`
/// path rather than passing vacuously.
#[derive(Default)]
struct DeadProjectionCollector {
    dead: Vec<String>,
    expression_scans: usize,
}

impl SExprVisitor for DeadProjectionCollector {
    fn visit(&mut self, s_expr: &SExpr) -> Result<VisitAction> {
        match s_expr.plan() {
            RelOperator::ExpressionScan(_) => self.expression_scans += 1,
            RelOperator::EvalScalar(eval_scalar) => {
                let child_outputs = s_expr
                    .child(0)?
                    .derive_relational_prop()?
                    .output_columns
                    .clone();
                for item in &eval_scalar.items {
                    let ScalarExpr::BoundColumnRef(column_ref) = &item.scalar else {
                        continue;
                    };
                    if column_ref.column.index == item.index && !child_outputs.contains(&item.index)
                    {
                        self.dead.push(item.index.to_string());
                    }
                }
            }
            _ => {}
        }
        Ok(VisitAction::Continue)
    }
}

/// `Binder::construct_expression_scan` projects every derived outer column of a flattened
/// lateral `VALUES` as an identity `EvalScalar` item, then prunes the cache columns the
/// values do not use, which leaves some of those items referencing a column nothing
/// computes. The binder drops them; this asserts the post-condition directly, because
/// the scope check that would otherwise catch a regression is compiled out of release
/// builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lateral_values_drops_unresolvable_projection_items() -> Result<()> {
    let cases = [
        // A literal first column leaves `t1.a` projected by the flattening but pruned from
        // the rebuilt join, so the identity item for it cannot be resolved.
        SqlTestCase {
            name: "left_lateral_values_literal_column",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t1.a, v1.c1 FROM scope_t t1
                LEFT JOIN LATERAL (VALUES ('b', t1.b), ('c', t1.c)) AS v1(tag, c1)
                ON t1.a = v1.c1",
        },
        SqlTestCase {
            name: "left_lateral_values_literal_column_join_on_literal",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t1.a, v1.c1 FROM scope_t t1
                LEFT JOIN LATERAL (VALUES ('b', t1.b), ('c', t1.c)) AS v1(tag, c1)
                ON t1.a = v1.tag",
        },
        SqlTestCase {
            name: "inner_lateral_values_literal_column",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t1.a, v1.c1 FROM scope_t t1
                JOIN LATERAL (VALUES ('b', t1.b), ('c', t1.c)) AS v1(tag, c1)
                ON t1.a = v1.c1",
        },
        SqlTestCase {
            name: "lateral_values_only_outer_columns",
            description: "",
            setup_sqls: &[TABLE],
            sql: "SELECT t1.a, v1.c1 FROM scope_t t1
                JOIN LATERAL (VALUES (t1.b, t1.c)) AS v1(c1, c2)
                ON t1.a = v1.c1",
        },
    ];
    for case in cases {
        let ctx = setup_context(&case).await?;
        let plan = ctx.bind_sql(case.sql).await?;
        let Plan::Query { s_expr, .. } = ctx.optimize_plan(plan).await? else {
            unreachable!("expected a query plan");
        };

        let mut collector = DeadProjectionCollector::default();
        s_expr.accept(&mut collector)?;
        // Proves the case reached the lateral `VALUES` branch of the binder.
        assert!(
            collector.expression_scans > 0,
            "{}: expected a lateral VALUES expression scan",
            case.name
        );
        assert!(
            collector.dead.is_empty(),
            "{}: kept projection items no input produces: {:?}",
            case.name,
            collector.dead
        );
    }
    Ok(())
}
