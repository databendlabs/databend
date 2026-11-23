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
use databend_common_meta_app::schema::DmlDeduplicationId;
use databend_common_meta_app::schema::DmlDeduplicationOperation;
use databend_common_meta_app::tenant::Tenant;
use databend_common_version::BUILD_INFO;
use databend_query::sessions::SessionManager;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::OSSSetup;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use futures::TryStreamExt;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_temp_table_deduplication_is_transaction_local() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom_and_session_type(
        OSSSetup {
            config: ConfigBuilder::create().config(),
        },
        SessionType::MySQL,
    )
    .await?;
    fixture.create_default_database().await?;
    let table = format!("{}.temp_writes", fixture.default_db_name());
    fixture
        .execute_command(&format!("CREATE TEMP TABLE {table}(id INT)"))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_default_catalog()?;
    let info = ctx
        .get_table("default", &fixture.default_db_name(), "temp_writes")
        .await?
        .get_table_info()
        .clone();
    let tenant = fixture.default_tenant();
    let principal = fixture.default_session().get_current_user()?.identity();
    let deduplication_id = |label: &str| DmlDeduplicationId {
        principal: principal.clone(),
        catalog_id: info.catalog_info.id.catalog_id,
        table_id: info.ident.table_id,
        operation: DmlDeduplicationOperation::Insert,
        token: label.to_owned().try_into().unwrap(),
    };
    let insert = |label, id| {
        format!("INSERT /*+ SET_VAR(deduplicate_label='{label}') */ INTO {table} VALUES ({id})")
    };

    fixture.execute_command(&insert("autocommit", 1)).await?;
    assert!(
        !catalog
            .has_dml_deduplication_marker(&tenant, &deduplication_id("autocommit"))
            .await?
    );

    fixture.execute_command("BEGIN").await?;
    let statement = insert("transaction", 2);
    fixture.execute_command(&statement).await?;
    fixture.execute_command(&statement).await?;
    fixture.execute_command("COMMIT").await?;
    assert!(
        !catalog
            .has_dml_deduplication_marker(&tenant, &deduplication_id("transaction"))
            .await?
    );

    let blocks = fixture
        .execute_query(&format!("SELECT id FROM {table} ORDER BY id"))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let ids = blocks
        .iter()
        .flat_map(|block| {
            let ids = block.get_by_offset(0);
            (0..block.num_rows()).map(move |row| ids.index(row).unwrap().to_string())
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["1", "2"]);

    fixture
        .execute_command(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}
