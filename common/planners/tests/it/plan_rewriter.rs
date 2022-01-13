// Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;

use common_exception::Result;
use common_planners::*;

use crate::test::Test;

#[test]
fn test_rewrite_projection_alias_plan() -> Result<()> {
    use pretty_assertions::assert_eq;

    struct RewriteTest {
        name: &'static str,
        exprs: Vec<Expression>,
        expect_str: &'static str,
        error_msg: &'static str,
    }

    let tests = vec![
        RewriteTest {
            name: "Cyclic",
            exprs: vec![
                Box::new(Expression::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("z")],
                })
                .alias("x"),
                Box::new(Expression::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("x")],
                })
                .alias("y"),
                Box::new(Expression::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("y")],
                })
                .alias("z"),
            ],
            expect_str: "",
            error_msg: "Code: 1005, displayText = Planner Error: Cyclic aliases: x.",
        },
        RewriteTest {
            name: "Duplicate aliases",
            exprs: vec![
                Box::new(Expression::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("z")],
                })
                .alias("x"),
                Box::new(Expression::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("y")],
                })
                .alias("x"),
            ],
            expect_str: "",
            error_msg:
                "Code: 1005, displayText = Planner Error: Different expressions with the same alias x.",
        },
        RewriteTest {
            name: "normal",
            exprs: vec![
                col("x"),
                Box::new(Expression::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), col("x")],
                })
                .alias("y"),
                Expression::ScalarFunction {
                    op: "multiply".to_string(),
                    args: vec![col("y"), col("y")],
                },
            ],
            expect_str: "[x, add(1, x) as y, multiply(add(1, x), add(1, x))]",
            error_msg: "",
        },
        RewriteTest {
            name: "normal2",
            exprs: vec![
                Box::new(Expression::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), lit(1i64)],
                })
                .alias("x"),
                Box::new(Expression::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), col("x")],
                })
                .alias("y"),
                Expression::ScalarFunction {
                    op: "multiply".to_string(),
                    args: vec![col("x"), col("y")],
                },
            ],
            expect_str:
                "[add(1, 1) as x, add(1, add(1, 1)) as y, multiply(add(1, 1), add(1, add(1, 1)))]",
            error_msg: "",
        },
        RewriteTest {
            name: "x+1->x",
            exprs: vec![
                Box::new(Expression::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![col("x"), lit(1i64)],
                })
                .alias("x"),
                Box::new(Expression::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), col("x")],
                })
                .alias("y"),
                Expression::ScalarFunction {
                    op: "multiply".to_string(),
                    args: vec![col("x"), col("y")],
                },
            ],
            expect_str:
                "[add(x, 1) as x, add(1, add(x, 1)) as y, multiply(add(x, 1), add(1, add(x, 1)))]",
            error_msg: "",
        },
    ];

    for t in tests {
        let result = RewriteHelper::rewrite_projection_aliases(&t.exprs);
        match &result {
            Ok(v) => assert_eq!(t.expect_str, format!("{:?}", v), "in test_case {}", t.name),
            Err(e) => assert_eq!(t.error_msg, e.to_string(), "in test_case {}", t.name),
        }
    }

    Ok(())
}

#[test]
fn test_rewrite_expressions_plan() -> Result<()> {
    use pretty_assertions::assert_eq;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .project(&[col("number").alias("x"), col("number").alias("y")])?
        .filter(col("x").eq(lit(1i64)))?
        .build()?;

    let actual = RewriteHelper::projection_to_map(&plan)?;
    let mut expect = HashMap::new();
    expect.insert("x".to_string(), col("number"));
    expect.insert("y".to_string(), col("number"));
    assert_eq!(expect, actual);

    let exprs = vec![Expression::ScalarFunction {
        op: "multiply".to_string(),
        args: vec![col("x"), col("y")],
    }];

    let expect_plan = Expression::ScalarFunction {
        op: "multiply".to_string(),
        args: vec![col("number"), col("number")],
    };
    let actual_plan = RewriteHelper::rewrite_alias_exprs(&actual, &exprs)?;
    assert_eq!(expect_plan, actual_plan[0]);

    Ok(())
}

struct DefaultRewriter;

impl PlanRewriter for DefaultRewriter {
    fn rewrite_aggregate_partial(
        &mut self,
        plan: &AggregatorPartialPlan,
    ) -> common_exception::Result<PlanNode> {
        Ok(PlanNode::AggregatorPartial(AggregatorPartialPlan {
            schema: plan.schema.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?),
        }))
    }

    fn rewrite_aggregate_final(
        &mut self,
        plan: &AggregatorFinalPlan,
    ) -> common_exception::Result<PlanNode> {
        Ok(PlanNode::AggregatorFinal(AggregatorFinalPlan {
            schema: plan.schema.clone(),
            schema_before_group_by: plan.schema_before_group_by.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?),
        }))
    }
}

#[test]
fn test_plan_rewriter_1() -> Result<()> {
    use common_planners::*;
    use pretty_assertions::assert_eq;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .filter(col("number").eq(lit(1i64)))?
        .aggregate_partial(&[col("number")], &[col("number")])?
        .aggregate_final(source.schema(), &[col("number")], &[col("number")])?
        .project(&[col("number").alias("x"), col("number").alias("y")])?
        .build()?;

    // DefaultRewriter::rewrite_plan_node() should return a totally same plan as input
    let mut rewriter = DefaultRewriter {};
    let before_rewrite = format!("{:?}", &plan);
    let after_rewrite = format!("{:?}", rewriter.rewrite_plan_node(&plan)?);
    assert_eq!(before_rewrite, after_rewrite);
    Ok(())
}
