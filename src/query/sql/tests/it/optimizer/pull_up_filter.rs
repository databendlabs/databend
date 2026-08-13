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

use databend_common_exception::Result;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::optimizers::operator::PullUpFilterOptimizer;
use databend_common_sql::plans::DerivedFrom;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::setup_context;

/// Optimizer-derived predicates must not be pulled up: they are only
/// guaranteed by an operator below them (e.g. a join that discards NULL
/// keys), and moving them away from that operator risks misusing them in
/// inference or join rewrites.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pull_up_keeps_derived_predicates_in_place() -> Result<()> {
    let case = SqlTestCase {
        name: "derived_predicates_stay_in_place",
        description: "",
        setup_sqls: &["CREATE TABLE t (a INT32 NULL)"],
        sql: "SELECT a FROM t WHERE a > 1",
    };
    let ctx = setup_context(&case).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(case.sql).await?
    else {
        unreachable!("test query should bind to Plan::Query")
    };

    // Inject a derived `is_not_null(a)` into the plan's Filter, mimicking what
    // a rule like null_addition produces below a join.
    let s_expr = inject_derived_predicate(&s_expr)?;

    let opt_ctx = OptimizerContext::new(ctx, metadata.clone());
    let mut optimizer = PullUpFilterOptimizer::new(opt_ctx);
    let optimized = optimizer.optimize_sync(&s_expr)?;

    // The user predicate `a > 1` is collected for inference and re-applied at
    // the root; the derived predicate must stay in its own Filter directly
    // above the Scan.
    let root = optimized
        .plan()
        .as_filter()
        .expect("root should be a Filter");
    assert!(
        root.predicates.iter().all(|p| !p.is_derived()),
        "pulled-up predicates must not contain derived ones: {:?}",
        root.predicates
    );
    let kept =
        find_derived_filter(&optimized).expect("derived predicate should remain in a Filter");
    assert!(
        matches!(kept.child(0)?.plan(), RelOperator::Scan(_)),
        "the derived predicate should stay right above the scan"
    );
    Ok(())
}

fn derived_is_not_null(arg: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(
        FunctionCall::new(None, "is_not_null".to_string(), vec![], vec![arg])
            .derived(DerivedFrom::NullAddition),
    )
}

/// A conjunction scalar that mixes a derived conjunct with a user conjunct
/// has an untagged root (`and_filters` is not itself derived). The derived
/// conjunct must still be classified per conjunct and stay in place, not
/// leak into the pulled-up inference set.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pull_up_splits_mixed_derived_conjunction() -> Result<()> {
    let case = SqlTestCase {
        name: "mixed_derived_conjunction",
        description: "",
        setup_sqls: &["CREATE TABLE t (a INT32 NULL)"],
        sql: "SELECT a FROM t WHERE a > 1",
    };
    let ctx = setup_context(&case).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(case.sql).await?
    else {
        unreachable!("test query should bind to Plan::Query")
    };

    // Replace the user Filter's predicates with a single conjunction scalar
    // mixing a derived conjunct with the user conjunct:
    //   and_filters(is_not_null(a)[derived], a > 1)
    let s_expr = inject_mixed_derived_conjunction(&s_expr)?;

    let opt_ctx = OptimizerContext::new(ctx, metadata.clone());
    let mut optimizer = PullUpFilterOptimizer::new(opt_ctx);
    let optimized = optimizer.optimize_sync(&s_expr)?;

    // The user conjunct is pulled to the root; the derived conjunct must
    // neither enter the pulled-up set nor be re-applied at the root.
    let root = optimized
        .plan()
        .as_filter()
        .expect("root should be a Filter");
    assert!(
        root.predicates.iter().all(|p| !p.is_derived()),
        "pulled-up predicates must not contain derived ones: {:?}",
        root.predicates
    );
    assert!(
        !root.predicates.iter().any(is_is_not_null),
        "the derived conjunct must not be re-applied at the root: {:?}",
        root.predicates
    );
    let kept = find_derived_filter(&optimized).expect("derived conjunct should remain in a Filter");
    assert!(
        matches!(kept.child(0)?.plan(), RelOperator::Scan(_)),
        "the derived conjunct should stay right above the scan"
    );
    Ok(())
}

/// Replace the first Filter's predicates with a single conjunction scalar
/// mixing a derived `is_not_null(<its column>)` with the original user
/// conjunct: `and_filters(is_not_null(a)[derived], <user predicate>)`.
fn inject_mixed_derived_conjunction(s_expr: &SExpr) -> Result<SExpr> {
    if let RelOperator::Filter(filter) = s_expr.plan()
        && let Some(column) = filter.predicates.iter().find_map(first_column_ref)
    {
        let mut filter = filter.clone();
        let user_conjunct = filter.predicates[0].clone();
        filter.predicates = vec![ScalarExpr::FunctionCall(FunctionCall::new(
            None,
            "and_filters".to_string(),
            vec![],
            vec![derived_is_not_null(column), user_conjunct],
        ))];
        return Ok(s_expr.replace_plan(Arc::new(RelOperator::Filter(filter))));
    }
    let children = s_expr
        .children()
        .map(inject_mixed_derived_conjunction)
        .collect::<Result<Vec<_>>>()?;
    Ok(s_expr.replace_children(children.into_iter().map(Arc::new)))
}

fn is_is_not_null(expr: &ScalarExpr) -> bool {
    matches!(expr, ScalarExpr::FunctionCall(f) if f.func_name == "is_not_null")
}

fn first_column_ref(expr: &ScalarExpr) -> Option<ScalarExpr> {
    match expr {
        ScalarExpr::BoundColumnRef(_) => Some(expr.clone()),
        ScalarExpr::FunctionCall(func) => func.arguments.iter().find_map(first_column_ref),
        ScalarExpr::CastExpr(cast) => first_column_ref(&cast.argument),
        _ => None,
    }
}

/// Find the first Filter and add a derived `is_not_null(<its column>)` to it.
fn inject_derived_predicate(s_expr: &SExpr) -> Result<SExpr> {
    if let RelOperator::Filter(filter) = s_expr.plan()
        && let Some(column) = filter.predicates.iter().find_map(first_column_ref)
    {
        let mut filter = filter.clone();
        filter.predicates.push(derived_is_not_null(column));
        return Ok(s_expr.replace_plan(Arc::new(RelOperator::Filter(filter))));
    }
    let children = s_expr
        .children()
        .map(inject_derived_predicate)
        .collect::<Result<Vec<_>>>()?;
    Ok(s_expr.replace_children(children.into_iter().map(Arc::new)))
}

fn find_derived_filter(s_expr: &SExpr) -> Option<SExpr> {
    if let RelOperator::Filter(filter) = s_expr.plan()
        && filter.predicates.iter().any(|p| p.is_derived())
    {
        return Some(s_expr.clone());
    }
    s_expr.children().find_map(find_derived_filter)
}
