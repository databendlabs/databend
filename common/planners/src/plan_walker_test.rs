// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::test::Test;
use crate::*;

#[test]
fn test_plan_walker() -> std::result::Result<(), Box<dyn std::error::Error>> {
    use pretty_assertions::assert_eq;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .aggregate_partial(&[sum(col("number")).alias("sumx")], &[])?
        .aggregate_final(&[sum(col("number")).alias("sumx")], &[])?
        .project(&[col("sumx")])?
        .build()?;

    // PreOrder.
    {
        let mut actual: Vec<String> = vec![];
        for child in plan.inputs() {
            child.walk_preorder(|plan| -> Result<bool, Box<dyn std::error::Error>> {
                actual.push(plan.name().to_string());
                return Ok(true);
            })?;
        }

        let expect = vec![
            "AggregatorFinalPlan".to_string(),
            "AggregatorPartialPlan".to_string(),
            "ReadSourcePlan".to_string(),
        ];
        assert_eq!(expect, actual);
    }

    // PostOrder.
    {
        let mut actual: Vec<String> = vec![];
        for child in plan.inputs() {
            child.walk_postorder(|plan| -> Result<bool, Box<dyn std::error::Error>> {
                actual.push(plan.name().to_string());
                return Ok(true);
            })?;
        }

        let expect = vec![
            "ReadSourcePlan".to_string(),
            "AggregatorPartialPlan".to_string(),
            "AggregatorFinalPlan".to_string(),
        ];
        assert_eq!(expect, actual);
    }

    Ok(())
}
