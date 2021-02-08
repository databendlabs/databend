// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_rewriter_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::datavalues::*;
    use crate::planners::*;

    #[allow(dead_code)]
    struct RewriteTest {
        name: &'static str,
        exprs: Vec<ExpressionPlan>,
        expect_str: &'static str,
        error_msg: &'static str,
    }

    let tests = vec![
        RewriteTest{
            name : "Cyclic",
            exprs: vec![
                ExpressionPlan::Alias(
                    "x".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("z".to_string()),
                        ],
                    }),
                ),
                ExpressionPlan::Alias(
                    "y".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("x".to_string()),
                        ],
                    }),
                ),
                ExpressionPlan::Alias(
                    "z".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("y".to_string()),
                        ],
                    }),
                ),
            ],
            expect_str: "",
            error_msg : "Error during plan: Cyclic aliases: x",
        },

        RewriteTest{
            name : "Duplicate aliases",
            exprs: vec![
                ExpressionPlan::Alias(
                    "x".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("z".to_string()),
                        ],
                    }),
                ),
                ExpressionPlan::Alias(
                    "x".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("y".to_string()),
                        ],
                    }),
                ),
            ],
            expect_str: "",
            error_msg : "Error during plan: Different expressions with the same alias x",
        },

        RewriteTest{
            name: "normal",
            exprs: vec![
                ExpressionPlan::Field("x".to_string()),
                ExpressionPlan::Alias(
                    "y".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("x".to_string()),
                        ],
                    }),
                ),
                ExpressionPlan::Function {
                    op: "multiply".to_string(),
                    args: vec![
                        ExpressionPlan::Field("y".to_string()),
                        ExpressionPlan::Field("y".to_string()),
                    ],
                },
            ],
            expect_str: "[x, add([1, x]) as y, multiply([add([1, x]), add([1, x])])]",
            error_msg: "",
        },

        RewriteTest{
            name: "normal2",
            exprs: vec![
                ExpressionPlan::Alias(
                    "x".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Constant(DataValue::Int64(Some(1i64))),
                        ],
                    }),
                ),
                ExpressionPlan::Alias(
                    "y".to_string(),
                    Box::new(ExpressionPlan::Function {
                        op: "add".to_string(),
                        args: vec![
                            ExpressionPlan::Constant(DataValue::Int32(Some(1i32))),
                            ExpressionPlan::Field("x".to_string()),
                        ],
                    }),
                ),
                ExpressionPlan::Function {
                    op: "multiply".to_string(),
                    args: vec![
                        ExpressionPlan::Field("x".to_string()),
                        ExpressionPlan::Field("y".to_string()),
                    ],
                },
            ],
            expect_str: "[add([1, 1]) as x, add([1, add([1, 1])]) as y, multiply([add([1, 1]), add([1, add([1, 1])])])]",
            error_msg: "",
        },
    ];

    for t in tests {
        let result = PlanRewriter::exprs_extract_aliases(t.exprs);
        match &result {
            Ok(v) => assert_eq!(t.expect_str, format!("{:?}", v), "in test_case {}", t.name),
            Err(e) => assert_eq!(t.error_msg, e.to_string(), "in test_case {}", t.name),
        }
    }

    Ok(())
}
