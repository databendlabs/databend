// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_rewriter_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::datavalues::*;
    use crate::planners::*;

    let exprs = vec![
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
    ];

    let new_exprs = PlanRewriter::exprs_extract_aliases(exprs)?;

    assert_eq!(
        "[x, add([1, x]) as y, multiply([add([1, x]), add([1, x])])]".to_string(),
        format!("{:?}", new_exprs)
    );
    Ok(())
}
