// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_limit_plan() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;

    let limit = PlanNode::Limit(LimitPlan {
        n: 33,
        input: Arc::from(PlanBuilder::empty(ctx).build()?),
    });
    let expect = "Limit: 33";
    let actual = format!("{:?}", limit);
    assert_eq!(expect, actual);
    Ok(())
}
