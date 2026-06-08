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

use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let mut session = TestFixture::create_dummy_session().await;

    // Tenant.
    {
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");

        // We are not in management mode, so always get the config tenant.
        assert!(
            session
                .set_current_tenant(Tenant::new_literal("tenant2"))
                .await
                .is_err()
        );
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");

        session
            .set_current_tenant(Tenant::new_literal("test"))
            .await?;
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");
    }

    // Settings.
    {
        let settings = session.get_settings();
        settings.set_max_threads(3)?;
        let actual = settings.get_max_threads()?;
        assert_eq!(actual, 3);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_in_management_mode() -> anyhow::Result<()> {
    let config = ConfigBuilder::create().with_management_mode().build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let mut session = TestFixture::create_dummy_session().await;

    // Tenant.
    {
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");

        session
            .set_current_tenant(Tenant::new_literal("tenant2"))
            .await?;
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "tenant2");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_settings_follow_management_tenant() -> anyhow::Result<()> {
    let config = ConfigBuilder::create().with_management_mode().build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let tenant = Tenant::new_literal("tenant2");
    let tenant_settings = Settings::create(tenant.clone());
    tenant_settings
        .set_global_setting("network_policy".to_string(), "policy_tenant2".to_string())
        .await?;

    let mut session = TestFixture::create_dummy_session().await;
    assert_eq!(session.get_current_tenant().tenant_name(), "test");
    assert_eq!(
        session
            .get_settings()
            .get_network_policy()
            .unwrap_or_default(),
        ""
    );

    session.set_current_tenant(tenant).await?;

    assert_eq!(session.get_current_tenant().tenant_name(), "tenant2");
    assert_eq!(
        session.get_settings().get_network_policy()?,
        "policy_tenant2"
    );

    Ok(())
}
