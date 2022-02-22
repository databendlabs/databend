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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::*;

use crate::test::Test;

#[test]
fn test_expression_plan_format() -> Result<()> {
    use pretty_assertions::assert_eq;

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", Vu8::to_data_type())]);

    let empty_plan = EmptyPlan::create_with_schema(schema.clone());
    let expression = PlanNode::Expression(ExpressionPlan {
        exprs: vec![col("a")],
        schema,
        input: Arc::from(PlanBuilder::from(&PlanNode::Empty(empty_plan)).build()?),
        desc: "".to_string(),
    });
    let _ = expression.schema();
    let expect = "Expression: a:String ()";
    let actual = format!("{:?}", expression);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_expression_plan() -> Result<()> {
    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .filter(
            add(col("number"), lit(1u8))
                .eq(lit(4u8))
                .and(col("number").not_eq(lit(4u8)))
                .and(col("number").lt(lit(4u8)))
                .and(col("number").lt_eq(lit(4u8)))
                .and(col("number").gt(lit(4u8)))
                .and(not(col("number").gt_eq(lit(4u8)))),
        )?
        .expression(&[modular(col("number"), lit(10u8))], "Expressions")?
        .build()?;
    let explain = PlanNode::Explain(ExplainPlan {
        typ: ExplainType::Syntax,
        input: Arc::new(plan),
    });
    let expect ="Expression: (number % 10):UInt8 (Expressions)\
    \n  Filter: (((((((number + 1) = 4) and (number != 4)) and (number < 4)) and (number <= 4)) and (number > 4)) and (not (number >= 4)))\
    \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000, partitions_scanned: 8, partitions_total: 8]";
    let actual = format!("{:?}", explain);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_expression_validate() -> Result<()> {
    struct Test {
        desc: &'static str,
        expression: Expression,
        error: Option<ErrorCode>,
    }

    let cases = vec![
        Test {
            desc: "toTypeName-not-pass",
            expression: Expression::ScalarFunction {
                op: "toTypeName".to_string(),
                args: vec![],
            },
            error: Some(ErrorCode::NumberArgumentsNotMatch(
                "Function `toTypeName` expect to have 1 arguments, but got 0",
            )),
        },
        Test {
            desc: "example-not-pass",
            expression: Expression::ScalarFunction {
                op: "example".to_string(),
                args: vec![col("33")],
            },
            error: Some(ErrorCode::NumberArgumentsNotMatch(
                "Function `example` expect to have 0 arguments, but got 1",
            )),
        },
        Test {
            desc: "example-pass",
            expression: Expression::ScalarFunction {
                op: "example".to_string(),
                args: vec![],
            },
            error: None,
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
            None => assert!(result.is_ok(), "{}", t.desc),
        }
    }
    Ok(())
}
