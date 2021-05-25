// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_expression_plan() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::*;

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
