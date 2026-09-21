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

use databend_common_catalog::session_type::SessionType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::SessionManager;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;

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
                .is_err()
        );
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");

        session.set_current_tenant(Tenant::new_literal("test"))?;
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

        session.set_current_tenant(Tenant::new_literal("tenant2"))?;
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "tenant2");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_local_session_can_override_tenant() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let mut session = SessionManager::instance()
        .create_session(SessionType::Local)
        .await?;

    session.set_current_tenant(Tenant::new_literal("tenant2"))?;
    let actual = session.get_current_tenant();
    assert_eq!(actual.tenant_name(), "tenant2");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_failed_temp_ctas_cleans_staged_table() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;
    let ctx = session.create_query_context(&BUILD_INFO).await?;

    for _ in 0..2 {
        let err = execute_command(
            ctx.clone(),
            "CREATE TEMP TABLE t AS SELECT number / 0 FROM numbers(1)",
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), 1006);
    }

    let temp_tbl_mgr = session.temp_tbl_mgr();
    let tables = temp_tbl_mgr.lock().list_tables()?;
    assert!(tables.is_empty());
    assert!(temp_tbl_mgr.lock().is_empty());

    Ok(())
}
