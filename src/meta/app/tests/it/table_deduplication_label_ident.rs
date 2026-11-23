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

use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::schema::DmlDeduplicationId;
use databend_common_meta_app::schema::DmlDeduplicationOperation;
use databend_common_meta_app::schema::DmlDeduplicationToken;
use databend_common_meta_app::schema::TableDeduplicationLabelIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::kvapi::testing::assert_round_trip;

#[test]
fn test_dml_deduplication_key_separates_scope_components() {
    let tenant = Tenant::new_literal("tenant/a");
    let id = DmlDeduplicationId {
        principal: UserIdentity::new("user/x", "%"),
        catalog_id: 0,
        table_id: 42,
        operation: DmlDeduplicationOperation::Insert,
        token: "load/1%".to_owned().try_into().unwrap(),
    };
    let key = id.key(&tenant);
    assert_round_trip(
        key.clone(),
        "__fd_dml_deduplication_v1/tenant%2fa/user%2fx/%25/0/42/insert/load%2f1%25",
    );
    assert_ne!(key, id.key(&Tenant::new_literal("tenant/b")));
    for different in [
        DmlDeduplicationId {
            principal: UserIdentity::new("user", "x/%"),
            ..id.clone()
        },
        DmlDeduplicationId {
            catalog_id: 1,
            ..id.clone()
        },
        DmlDeduplicationId {
            table_id: 43,
            ..id.clone()
        },
        DmlDeduplicationId {
            operation: DmlDeduplicationOperation::Update,
            ..id.clone()
        },
    ] {
        assert_ne!(key.to_string_key(), different.key(&tenant).to_string_key());
    }
}

#[test]
fn test_dml_deduplication_token_round_trip_and_byte_limit() {
    let boundary = format!(
        "/{}/",
        "é".repeat((DmlDeduplicationToken::MAX_BYTES - 2) / 2)
    );
    let token = DmlDeduplicationToken::try_from(boundary.clone()).unwrap();
    let json = serde_json::to_string(&boundary).unwrap();
    assert_eq!(serde_json::to_string(&token).unwrap(), json);
    assert_eq!(
        serde_json::from_str::<DmlDeduplicationToken>(&json).unwrap(),
        token
    );

    for invalid in [String::new(), format!("{boundary}/")] {
        let json = serde_json::to_string(&invalid).unwrap();
        assert!(DmlDeduplicationToken::try_from(invalid).is_err());
        assert!(serde_json::from_str::<DmlDeduplicationToken>(&json).is_err());
    }
    assert!(
        DmlDeduplicationToken::from_setting(String::new())
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_table_deduplication_label_key() {
    let label = "daily/import%";
    let tenant_a = Tenant::new_literal("tenant_a");
    let tenant_b = Tenant::new_literal("tenant/b");

    assert_round_trip(
        TableDeduplicationLabelIdent::new(tenant_a, label),
        "__fd_table_deduplication_label/tenant_a/daily%2fimport%25",
    );
    assert_round_trip(
        TableDeduplicationLabelIdent::new(tenant_b, label),
        "__fd_table_deduplication_label/tenant%2fb/daily%2fimport%25",
    );
}
