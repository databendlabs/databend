// Copyright 2021 Datafuse Labs
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

use databend_common_expression::TableDataType;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFScript;
use databend_common_proto_conv::FromToProto;

use crate::common;

#[test]
fn test_v192_script_external_access() -> anyhow::Result<()> {
    let mut scalar = UDFScript {
        external_access_integrations: vec!["i".into()],
        code: "pass".into(),
        handler: "f".into(),
        language: "python".into(),
        arg_types: vec![],
        return_type: TableDataType::String,
        runtime_version: String::new(),
        imports: vec![],
        packages: vec![],
        immutable: None,
    };
    // Frozen v192 bytes. Security bindings must survive persistence, and an
    // older reader must refuse them instead of discarding unknown protobuf fields.
    let scalar_bytes = vec![
        10, 4, 112, 97, 115, 115, 18, 1, 102, 26, 6, 112, 121, 116, 104, 111, 110, 42, 10, 146, 2,
        0, 160, 6, 192, 1, 168, 6, 24, 82, 1, 105, 160, 6, 192, 1, 168, 6, 192, 1,
    ];
    common::test_load_old("v192 scalar", &scalar_bytes, 192, scalar.clone())?;
    common::test_pb_from_to("script network bindings", scalar.clone())?;
    assert_eq!(scalar.to_pb().min_reader_ver, 192);
    scalar.external_access_integrations.clear();
    assert_eq!(scalar.to_pb().min_reader_ver, 24);

    let mut aggregate = UDAFScript {
        external_access_integrations: vec!["i".into()],
        code: "pass".into(),
        language: "python".into(),
        arg_types: vec![],
        state_fields: vec![],
        return_type: TableDataType::String,
        runtime_version: String::new(),
        imports: vec![],
        packages: vec![],
    };
    let aggregate_bytes = vec![
        10, 4, 112, 97, 115, 115, 18, 6, 112, 121, 116, 104, 111, 110, 34, 10, 146, 2, 0, 160, 6,
        192, 1, 168, 6, 24, 74, 1, 105, 160, 6, 192, 1, 168, 6, 192, 1,
    ];
    common::test_load_old("v192 aggregate", &aggregate_bytes, 192, aggregate.clone())?;
    common::test_pb_from_to("aggregate network bindings", aggregate.clone())?;
    assert_eq!(aggregate.to_pb().min_reader_ver, 192);
    aggregate.external_access_integrations.clear();
    assert_eq!(aggregate.to_pb().min_reader_ver, 24);
    Ok(())
}
