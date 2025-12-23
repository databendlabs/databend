//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::time::Duration;

use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::protobuf::BooleanExpression;

#[test]
fn test_txn_request_serde() -> anyhow::Result<()> {
    // Empty operations, with condition
    let txn = TxnRequest::new(vec![TxnCondition::eq_value("k", b("v"))], vec![
        TxnOp::put_with_ttl("k", b("v"), Some(Duration::from_millis(100))),
    ]);
    let want = concat!(
        r#"{"#,
        r#""condition":[{"key":"k","expected":0,"target":{"Value":[118]}}],"#,
        r#""if_then":[{"request":{"Put":{"key":"k","value":[118],"prev_value":true,"expire_at":null,"ttl_ms":100}}}],"#,
        r#""else_then":[]"#,
        r#"}"#
    );
    assert_eq!(want, serde_json::to_string(&txn)?);
    assert_eq!(txn, serde_json::from_str(want)?);

    // Only operations

    let txn = TxnRequest::default().push_branch(
        Some(BooleanExpression::from_conditions_or([
            TxnCondition::eq_value("k", b("v")),
        ])),
        [TxnOp::get("k")],
    );
    let want = r#"{
  "operations": [
    {
      "predicate": {
        "operator": 1,
        "sub_expressions": [],
        "conditions": [
          {
            "key": "k",
            "expected": 0,
            "target": {
              "Value": [
                118
              ]
            }
          }
        ]
      },
      "operations": [
        {
          "request": {
            "Get": {
              "key": "k"
            }
          }
        }
      ]
    }
  ],
  "condition": [],
  "if_then": [],
  "else_then": []
}"#;
    assert_eq!(want, serde_json::to_string_pretty(&txn)?);
    assert_eq!(txn, serde_json::from_str(want)?);

    Ok(())
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}
