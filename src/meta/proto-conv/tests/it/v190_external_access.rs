// Copyright 2026 Datafuse Labs.
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
use databend_common_meta_app::principal::ExternalAccessIntegration;
use databend_common_meta_app::principal::NetworkRule;
use databend_common_meta_app::principal::UDFServer;
use databend_common_proto_conv::FromToProto;
use fastrace::func_name;

use crate::common;

// Frozen v190 bytes: do not regenerate when metadata evolves.
#[test]
fn test_decode_v190_external_access() -> anyhow::Result<()> {
    let rule = NetworkRule {
        name: "r".into(),
        value_list: vec!["example.com:443".into()],
    };
    let rule_bytes = vec![
        10, 1, 114, 18, 15, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111, 109, 58, 52, 52, 51,
        160, 6, 190, 1, 168, 6, 24,
    ];
    common::test_load_old(func_name!(), &rule_bytes, 190, rule.clone())?;
    common::test_pb_from_to(func_name!(), rule)?;
    let integration = ExternalAccessIntegration {
        name: "i".into(),
        allowed_network_rules: vec!["r".into()],
        enabled: true,
    };
    let integration_bytes = vec![10, 1, 105, 18, 1, 114, 24, 1, 160, 6, 190, 1, 168, 6, 24];
    common::test_load_old(func_name!(), &integration_bytes, 190, integration.clone())?;
    common::test_pb_from_to(func_name!(), integration)?;
    let udf = UDFServer {
        address: "https://example.com".into(),
        handler: "f".into(),
        language: "python".into(),
        arg_names: vec![],
        arg_types: vec![],
        return_type: TableDataType::String,
        headers: Default::default(),
        immutable: None,
        external_access_integrations: vec!["i".into()],
    };
    let udf_bytes = vec![
        10, 19, 104, 116, 116, 112, 115, 58, 47, 47, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111,
        109, 18, 1, 102, 26, 6, 112, 121, 116, 104, 111, 110, 42, 10, 146, 2, 0, 160, 6, 190, 1,
        168, 6, 24, 74, 1, 105, 160, 6, 190, 1, 168, 6, 190, 1,
    ];
    common::test_load_old(func_name!(), &udf_bytes, 190, udf.clone())?;
    common::test_pb_from_to(func_name!(), udf.clone())?;
    assert_eq!(udf.to_pb().min_reader_ver, 190);
    Ok(())
}
