// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;

use crate::*;

#[test]
fn test_plan_display_indent() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    // TODO test other plan type
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int64, false)]);

    let mut options = HashMap::new();
    options.insert("opt_foo".to_string(), "opt_bar".to_string());

    let plan_create = PlanNode::CreateTable(CreateTablePlan {
        if_not_exists: true,
        db: "foo".into(),
        table: "bar".into(),
        schema,
        engine: TableEngineType::JsonEachRaw,
        options,
    });

    assert_eq!(
        "Create table foo.bar DataField { name: \"a\", data_type: Int64, nullable: false }, engine: JSON, if_not_exists:true, option: {\"opt_foo\": \"opt_bar\"}",
        format!("{}", plan_create.display_indent())
    );

    Ok(())
}
