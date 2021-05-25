// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use crate::test::Test;
use crate::*;

#[test]
fn test_rewrite_projection_alias_plan() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    #[allow(dead_code)]
    struct RewriteTest {
        name: &'static str,
        exprs: Vec<ExpressionAction>,
        expect_str: &'static str,
        error_msg: &'static str
    }

    let tests = vec![
        RewriteTest {
            name: "Cyclic",
            exprs: vec![
                Box::new(ExpressionAction::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("z")]
                })
                .alias("x"),
                Box::new(ExpressionAction::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("x")]
                })
                .alias("y"),
                Box::new(ExpressionAction::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("y")]
                })
                .alias("z"),
            ],
            expect_str: "",
            error_msg: "Code: 5, displayText = Planner Error: Cyclic aliases: x."
        },
        RewriteTest {
            name: "Duplicate aliases",
            exprs: vec![
                Box::new(ExpressionAction::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("z")]
                })
                .alias("x"),
                Box::new(ExpressionAction::ScalarFunction {
                    op: "plus".to_string(),
                    args: vec![lit(1i32), col("y")]
                })
                .alias("x"),
            ],
            expect_str: "",
            error_msg:
                "Code: 5, displayText = Planner Error: Different expressions with the same alias x."
        },
        RewriteTest {
            name: "normal",
            exprs: vec![
                col("x"),
                Box::new(ExpressionAction::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), col("x")]
                })
                .alias("y"),
                ExpressionAction::ScalarFunction {
                    op: "multiply".to_string(),
                    args: vec![col("y"), col("y")]
                },
            ],
            expect_str: "[x, add(1, x) as y, multiply(add(1, x), add(1, x))]",
            error_msg: ""
        },
        RewriteTest {
            name: "normal2",
            exprs: vec![
                Box::new(ExpressionAction::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), lit(1i64)]
                })
                .alias("x"),
                Box::new(ExpressionAction::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), col("x")]
                })
                .alias("y"),
                ExpressionAction::ScalarFunction {
                    op: "multiply".to_string(),
                    args: vec![col("x"), col("y")]
                },
            ],
            expect_str:
                "[add(1, 1) as x, add(1, add(1, 1)) as y, multiply(add(1, 1), add(1, add(1, 1)))]",
            error_msg: ""
        },
        RewriteTest {
            name: "x+1->x",
            exprs: vec![
                Box::new(ExpressionAction::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![col("x"), lit(1i64)]
                })
                .alias("x"),
                Box::new(ExpressionAction::ScalarFunction {
                    op: "add".to_string(),
                    args: vec![lit(1i32), col("x")]
                })
                .alias("y"),
                ExpressionAction::ScalarFunction {
                    op: "multiply".to_string(),
                    args: vec![col("x"), col("y")]
                },
            ],
            expect_str:
                "[add(x, 1) as x, add(1, add(x, 1)) as y, multiply(add(x, 1), add(1, add(x, 1)))]",
            error_msg: ""
        },
    ];

    for t in tests {
        let result = RewriteHelper::rewrite_projection_aliases(&t.exprs);
        match &result {
            Ok(v) => assert_eq!(t.expect_str, format!("{:?}", v), "in test_case {}", t.name),
            Err(e) => assert_eq!(t.error_msg, e.to_string(), "in test_case {}", t.name)
        }
    }

    Ok(())
}

#[test]
fn test_rewrite_expressions_plan() -> anyhow::Result<()> {
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

    let exprs = vec![ExpressionAction::ScalarFunction {
        op: "multiply".to_string(),
        args: vec![col("x"), col("y")]
    }];

    let expect_plan = ExpressionAction::ScalarFunction {
        op: "multiply".to_string(),
        args: vec![col("number"), col("number")]
    };
    let actual_plan = RewriteHelper::rewrite_alias_exprs(&actual, &exprs)?;
    assert_eq!(expect_plan, actual_plan[0]);

    Ok(())
}

#[test]
fn test_plan_rewriter_1() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::*;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .filter(col("number").eq(lit(1i64)))?
        .aggregate_partial(&[col("number")], &[col("number")])?
<<<<<<< HEAD
        .aggregate_final(&[col("number")], &[col("number")])?
=======
        .stage(String::from("hahaha"), StageState::AggregatorMerge)?
        .aggregate_final(source.schema(), &[col("number")], &[col("number")])?
>>>>>>> master
        .project(&[col("number").alias("x"), col("number").alias("y")])?
        .build()?;
    struct DefaultRewriter;
    impl<'plan> PlanRewriter<'plan> for DefaultRewriter {}

    // DefaultRewriter::rewrite_plan_node() should return a totally same plan as input
    let mut rewriter = DefaultRewriter {};
    let before_rewrite = format!("{:?}", &plan);
    let after_rewrite = format!("{:?}", rewriter.rewrite_plan_node(&plan)?);
    assert_eq!(before_rewrite, after_rewrite);
    Ok(())
}
