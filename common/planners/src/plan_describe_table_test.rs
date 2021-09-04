// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

#[test]
fn test_describe_table_plan() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("Field", DataType::String, false),
        DataField::new("Type", DataType::String, false),
        DataField::new("Null", DataType::String, false),
    ]);

    let describe = PlanNode::DescribeTable(DescribeTablePlan {
        db: "foo".into(),
        table: "bar".into(),
        schema,
    });

    let expect = "\
    DataSchema { fields: [\
        DataField { name: \"Field\", data_type: String, nullable: false }, \
        DataField { name: \"Type\", data_type: String, nullable: false }, \
        DataField { name: \"Null\", data_type: String, nullable: false }\
    ] }";
    let actual = format!("{:?}", describe.schema());
    assert_eq!(expect, actual);

    Ok(())
}
