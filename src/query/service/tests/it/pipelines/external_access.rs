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

use std::collections::BTreeMap;

use databend_common_exception::ErrorCode;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_sql::IndexType;
use databend_common_sql::plans::UDFType;
use databend_query::physical_plans::UdfFunctionDesc;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_value;
use serde_json::to_value;

// The pre-policy physical descriptor, as decoded by an old Flight worker.
// In particular, serde ignores unknown fields but requires `udf_type`.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyUdfFunctionDesc {
    name: String,
    func_name: String,
    output_column: Symbol,
    arg_indices: Vec<IndexType>,
    arg_exprs: Vec<String>,
    data_type: Box<DataType>,
    headers: BTreeMap<String, String>,
    udf_type: UDFType,
}

fn udf_desc(integrations: Vec<String>) -> UdfFunctionDesc {
    UdfFunctionDesc {
        external_access_integrations: integrations,
        name: "managed_echo".into(),
        func_name: "echo".into(),
        output_column: Symbol::new(1),
        arg_indices: vec![0],
        arg_exprs: vec!["arg".into()],
        data_type: Box::new(DataType::String),
        headers: BTreeMap::new(),
        udf_type: UDFType::Server("https://video.example.com".into()),
    }
}

#[test]
fn legacy_udf_plan_wire_compatibility() -> anyhow::Result<()> {
    let desc = udf_desc(vec![]);
    let mut wire = to_value(&desc)?;
    let legacy: LegacyUdfFunctionDesc = from_value(wire.clone())?;
    assert_eq!(legacy.udf_type, desc.udf_type);
    assert_eq!(from_value::<UdfFunctionDesc>(wire.clone())?, desc);
    // New workers must still decode plans from pre-policy coordinators.
    wire.as_object_mut()
        .unwrap()
        .remove("external_access_integrations");
    assert_eq!(from_value::<UdfFunctionDesc>(wire)?, desc);
    Ok(())
}

#[test]
fn policy_udf_plan_rejects_legacy_executor() -> anyhow::Result<()> {
    let desc = udf_desc(vec!["video_access".into()]);
    let wire = to_value(&desc)?;
    assert!(wire.get("udf_type").is_none());
    let err = from_value::<LegacyUdfFunctionDesc>(wire.clone()).unwrap_err();
    assert!(
        err.to_string().contains("missing field `udf_type`"),
        "{err}"
    );
    assert_eq!(from_value::<UdfFunctionDesc>(wire)?, desc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_access_ddl_in_management_mode() -> anyhow::Result<()> {
    let config = ConfigBuilder::create().with_management_mode().config();
    let fixture = TestFixture::setup_with_config(&config).await?;
    for sql in [
        "CREATE NETWORK RULE management_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('example.com:443')",
        "CREATE EXTERNAL ACCESS INTEGRATION management_access ALLOWED_NETWORK_RULES = (management_rule) ENABLED = TRUE",
        "ALTER NETWORK RULE management_rule SET VALUE_LIST = ('example.com:8443')",
        "ALTER EXTERNAL ACCESS INTEGRATION management_access SET ENABLED = FALSE",
    ] {
        fixture.execute_command(sql).await?;
    }
    let session = fixture.default_session();
    let admin = session.get_current_user()?;
    let mut user = UserInfo::new("unprivileged", "%", AuthInfo::None);
    user.grants.grant_role("public".into());
    session.set_authed_user(user, None).await?;
    session.set_current_role_checked("public").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;
    // Whitelisting the plan must not bypass the existing SUPER requirement.
    for sql in [
        "CREATE NETWORK RULE denied_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('example.com:443')",
        "CREATE EXTERNAL ACCESS INTEGRATION denied_access ALLOWED_NETWORK_RULES = (management_rule) ENABLED = TRUE",
        "ALTER NETWORK RULE management_rule SET VALUE_LIST = ('example.com:443')",
        "ALTER EXTERNAL ACCESS INTEGRATION management_access SET ENABLED = TRUE",
        "DROP NETWORK RULE management_rule",
        "DROP INTEGRATION management_access",
    ] {
        assert_eq!(
            fixture.execute_command(sql).await.unwrap_err().code(),
            ErrorCode::PERMISSION_DENIED,
            "{sql}"
        );
    }
    session.set_authed_user(admin, None).await?;
    fixture
        .execute_command("DROP INTEGRATION management_access")
        .await?;
    fixture
        .execute_command("DROP NETWORK RULE management_rule")
        .await?;
    Ok(())
}
