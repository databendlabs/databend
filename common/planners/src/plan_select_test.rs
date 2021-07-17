// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::*;

#[test]
fn test_select_wildcard_plan() -> Result<()> {
    use pretty_assertions::assert_eq;

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]);
    let plan = PlanBuilder::create(schema).project(&[col("a")])?.build()?;
    let select = PlanNode::Select(SelectPlan {
        input: Arc::new(plan),
    });
    let expect = "Projection: a:Utf8";

    let actual = format!("{:?}", select);
    assert_eq!(expect, actual);
    Ok(())
}
