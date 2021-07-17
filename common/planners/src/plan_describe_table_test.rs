// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

#[test]
fn test_describe_table_plan() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("Field", DataType::Utf8, false),
        DataField::new("Type", DataType::Utf8, false),
        DataField::new("Null", DataType::Utf8, false),
    ]);

    let describe = PlanNode::DescribeTable(DescribeTablePlan {
        db: "foo".into(),
        table: "bar".into(),
        schema,
    });

    let expect = "\
    DataSchema { fields: [\
        DataField { name: \"Field\", data_type: Utf8, nullable: false }, \
        DataField { name: \"Type\", data_type: Utf8, nullable: false }, \
        DataField { name: \"Null\", data_type: Utf8, nullable: false }\
    ] }";
    let actual = format!("{:?}", describe.schema());
    assert_eq!(expect, actual);

    Ok(())
}
