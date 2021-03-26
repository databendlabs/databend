// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_limit_plan() -> crate::error::PlannerResult<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::*;

    let limit = PlanNode::Limit(LimitPlan {
        n: 33,
        input: Arc::from(PlanBuilder::empty().build()?),
    });
    let expect = "Limit: 33";
    let actual = format!("{:?}", limit);
    assert_eq!(expect, actual);
    Ok(())
}
