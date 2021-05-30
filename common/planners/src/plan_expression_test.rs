// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::*;
use common_exception::ErrorCodes;

use crate::test::Test;
use crate::*;

#[test]
fn test_expression_plan_format() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]);

    let expression = PlanNode::Expression(ExpressionPlan {
        exprs: vec![col("a")],
        schema: schema.clone(),
        input: Arc::from(PlanBuilder::from(&PlanNode::Empty(EmptyPlan { schema })).build()?),
        desc: "".to_string()
    });
    let _ = expression.schema();
    let expect = "Expression: a:Utf8 ()";
    let actual = format!("{:?}", expression);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_expression_plan() -> anyhow::Result<()> {
    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .filter(
            add(col("number"), lit(1))
                .eq(lit(4))
                .and(col("number").not_eq(lit(4)))
                .and(col("number").lt(lit(4)))
                .and(col("number").lt_eq(lit(4)))
                .and(col("number").gt(lit(4)))
                .and(not(col("number").gt_eq(lit(4))))
        )?
        .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: ExplainType::Syntax,
        input: Arc::new(plan)
    });
    let expect ="Filter: (((((((number + 1) = 4) and (number != 4)) and (number < 4)) and (number <= 4)) and (number > 4)) and (not (number >= 4)))\
    \n  ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_expression_validate() -> anyhow::Result<()> {
    struct Test {
        desc: &'static str,
        expression: Expression,
        error: Option<ErrorCodes>
    }

    let cases = vec![
        Test {
            desc: "toTypeName-not-pass",
            expression: Expression::ScalarFunction {
                op: "toTypeName".to_string(),
                args: vec![]
            },
            error: Some(ErrorCodes::NumberArgumentsNotMatch(
                "ToTypeNameFunction expect to have 1 arguments, but got 0"
            ))
        },
        Test {
            desc: "example-not-pass",
            expression: Expression::ScalarFunction {
                op: "example".to_string(),
                args: vec![col("33")]
            },
            error: Some(ErrorCodes::NumberArgumentsNotMatch(
                "UdfExampleFunction expect to have 0 arguments, but got 1"
            ))
        },
        Test {
            desc: "example-pass",
            expression: Expression::ScalarFunction {
                op: "example".to_string(),
                args: vec![]
            },
            error: None
        },
    ];

    for t in cases.iter() {
        let result = validate_expression(&t.expression);
        match t.error {
            Some(_) => {
                assert_eq!(
                    t.error.as_ref().unwrap().message(),
                    result.err().unwrap().message(),
                    "{}",
                    t.desc
                );
            }
            None => assert!(result.is_ok(), "{}", t.desc)
        }
    }
    Ok(())
}
