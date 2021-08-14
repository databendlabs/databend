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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::*;

#[test]
fn test_scan_plan() -> Result<()> {
    use pretty_assertions::assert_eq;

    let scan = PlanNode::Scan(ScanPlan {
        schema_name: "scan_test".to_string(),
        table_id: 0,
        table_version: None,
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
