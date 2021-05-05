// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_scan_plan() -> anyhow::Result<()> {
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::*;

    let scan = PlanNode::Scan(ScanPlan {
        schema_name: "scan_test".to_string(),
        table_schema: DataSchemaRefExt::create_with_metadata(vec![DataField::new(
            "a",
            DataType::Utf8,
            false
        )]),
        table_args: None,
        projection: None,
        projected_schema: DataSchemaRefExt::create_with_metadata(vec![DataField::new(
            "a",
            DataType::Utf8,
            false
        )]),
        filters: vec![],
        limit: None
    });
    let _ = scan.schema();
    let expect = "";
    let actual = format!("{:?}", scan);
    assert_eq!(expect, actual);
    Ok(())
}
