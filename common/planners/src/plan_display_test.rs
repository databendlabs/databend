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

use std::collections::HashMap;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::TableMeta;

use crate::*;

#[test]
fn test_plan_display_indent() -> Result<()> {
    use pretty_assertions::assert_eq;

    // TODO test other plan type
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int64, false)]);

    let mut options = HashMap::new();
    options.insert("opt_foo".to_string(), "opt_bar".to_string());

    let plan_create = PlanNode::CreateTable(CreateTablePlan {
        if_not_exists: true,
        db: "foo".into(),
        table: "bar".into(),
        table_meta: TableMeta {
            schema,
            engine: "JSON".to_string(),
            options,
        },
    });

    assert_eq!(
        "Create table foo.bar DataField { name: \"a\", data_type: Int64, nullable: false }, engine: JSON, if_not_exists:true, option: {\"opt_foo\": \"opt_bar\"}",
        format!("{:?}", plan_create)
    );

    Ok(())
}
