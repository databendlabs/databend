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

use std::collections::HashMap;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableMeta;
use common_planners::*;

#[test]
fn test_plan_display_indent() -> Result<()> {
    use pretty_assertions::assert_eq;

    // TODO test other plan type
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i64::to_data_type())]);

    let mut options = HashMap::new();
    options.insert("opt_foo".to_string(), "opt_bar".to_string());

    let plan_create = PlanNode::CreateTable(CreateTablePlan {
        if_not_exists: true,
        tenant: "tenant1".into(),
        db: "foo".into(),
        table: "bar".into(),
        table_meta: TableMeta {
            schema,
            engine: "JSON".to_string(),
            options,
            ..Default::default()
        },
        as_select: None,
    });

    assert_eq!(
        "Create table foo.bar DataField { name: \"a\", data_type: Int64, nullable: false }, engine: JSON, if_not_exists:true, option: {\"opt_foo\": \"opt_bar\"}, as_select: None",
        format!("{:?}", plan_create)
    );

    Ok(())
}
