// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::*;

use crate::*;

#[test]
fn test_scan_plan() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    let scan = PlanNode::Scan(ScanPlan {
        schema_name: "scan_test".to_string(),
        table_schema: DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
        table_args: None,
        projected_schema: DataSchemaRefExt::create(vec![DataField::new(
            "a",
            DataType::Utf8,
            false,
        )]),
        push_downs: Extras::default(),
    });

    let _ = scan.schema();
    let expect = "";
    let actual = format!("{:?}", scan);
    assert_eq!(expect, actual);
    Ok(())
}
