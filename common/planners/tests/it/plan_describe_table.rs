// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use pretty_assertions::assert_eq;

#[test]
fn test_describe_table_plan() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("Field", Vu8::to_data_type()),
        DataField::new("Type", Vu8::to_data_type()),
        DataField::new("Null", Vu8::to_data_type()),
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
        DataField { name: \"Null\", data_type: String, nullable: false }], \
        metadata: {} \
    }";
    let actual = format!("{:?}", describe.schema());
    assert_eq!(expect, actual);

    Ok(())
}
