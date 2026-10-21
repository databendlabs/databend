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

use std::thread;
use std::time::Duration;

use anyhow::Result;
use databend_common_base::base::GlobalInstance;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_app::principal::ExternalAccessIntegration;
use databend_common_meta_app::principal::NetworkRule;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;
use databend_meta_client::types::MatchSeq;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_external_access_live_policy() -> Result<()> {
    let thread_name = thread::current().name().unwrap().to_string();
    GlobalInstance::init_testing(&thread_name);
    let mut config = InnerConfig::default();
    config.query.common.enable_udf_server = true;
    GlobalConfig::init(&config, &BUILD_INFO)?;
    let tenant = Tenant::new_literal("external_access_test");
    let provider = UserApiProvider::try_create_simple(RpcClientConf::empty(), &tenant).await?;
    let rules = provider.network_rule_api(&tenant);
    let integrations = provider.external_access_integration_api(&tenant);
    rules
        .add(
            NetworkRule {
                name: "video".into(),
                value_list: vec!["video.example.com:443".into()],
            },
            &CreateOption::Create,
        )
        .await?;
    rules
        .add(
            NetworkRule {
                name: "other".into(),
                value_list: vec!["other.example.com:8443".into()],
            },
            &CreateOption::Create,
        )
        .await?;
    let mut integration = ExternalAccessIntegration {
        name: "access".into(),
        allowed_network_rules: vec!["video".into(), "other".into()],
        enabled: true,
    };
    integrations
        .add(integration.clone(), &CreateOption::Create)
        .await?;
    let names = vec!["access".into()];
    let address = "https://video.example.com";
    let mut second = ExternalAccessIntegration {
        name: "second".into(),
        allowed_network_rules: vec!["other".into()],
        enabled: true,
    };
    integrations
        .add(second.clone(), &CreateOption::Create)
        .await?;
    let multiple = vec!["access".into(), "second".into()];
    provider
        .validate_external_access(&tenant, address, &multiple)
        .await?;
    second.enabled = false;
    integrations.update(second, MatchSeq::GE(1)).await?;
    sleep(Duration::from_millis(500)).await;
    assert!(
        provider
            .validate_external_access(&tenant, address, &multiple)
            .await
            .is_err()
    );
    provider
        .validate_external_access(&tenant, address, &names)
        .await?;
    provider
        .validate_external_access(&tenant, "https://other.example.com:8443", &names)
        .await?;
    for denied in [
        "https://video.example.com:444",
        "http://video.example.com",
        "https://video.example.com.evil",
        "https://other.example.com",
    ] {
        assert!(
            provider
                .validate_external_access(&tenant, denied, &names)
                .await
                .is_err(),
            "{denied}"
        );
    }
    assert!(
        provider
            .validate_external_access(&Tenant::new_literal("other_tenant"), address, &names)
            .await
            .is_err()
    );
    assert!(
        provider
            .validate_external_access(&tenant, address, &["missing".into()])
            .await
            .is_err()
    );
    integration.enabled = false;
    integrations
        .update(integration.clone(), MatchSeq::GE(1))
        .await?;
    sleep(Duration::from_millis(500)).await;
    assert!(
        provider
            .validate_external_access(&tenant, address, &names)
            .await
            .is_err()
    );
    integration.enabled = true;
    integrations
        .update(integration.clone(), MatchSeq::GE(1))
        .await?;
    sleep(Duration::from_millis(500)).await;
    provider
        .validate_external_access(&tenant, address, &names)
        .await?;
    rules
        .update(
            NetworkRule {
                name: "video".into(),
                value_list: vec!["video.example.com:8443".into()],
            },
            MatchSeq::GE(1),
        )
        .await?;
    sleep(Duration::from_millis(500)).await;
    assert!(
        provider
            .validate_external_access(&tenant, address, &names)
            .await
            .is_err()
    );
    provider
        .validate_external_access(&tenant, "https://video.example.com:8443", &names)
        .await?;
    integration.allowed_network_rules = vec!["other".into()];
    integrations.update(integration, MatchSeq::GE(1)).await?;
    sleep(Duration::from_millis(500)).await;
    assert!(
        provider
            .validate_external_access(&tenant, "https://video.example.com:8443", &names)
            .await
            .is_err()
    );
    rules.remove("other", MatchSeq::GE(1)).await?;
    sleep(Duration::from_millis(500)).await;
    assert!(
        provider
            .validate_external_access(&tenant, "https://other.example.com:8443", &names)
            .await
            .is_err()
    );
    integrations.remove("access", MatchSeq::GE(1)).await?;
    sleep(Duration::from_millis(500)).await;
    assert!(
        provider
            .validate_external_access(&tenant, address, &names)
            .await
            .is_err()
    );
    Ok(())
}
