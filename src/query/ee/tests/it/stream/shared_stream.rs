// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::NumberScalar;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_store::LocalMetaService;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_version::BUILD_INFO;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::Session;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableFactory;
use databend_query::stream::ReadDataBlockStream;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use databend_query::test_kits::execute_query;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_SHARED_DATABASE_ID;
use futures::TryStreamExt;

pub(super) async fn command(session: &Arc<Session>, sql: &str) -> Result<()> {
    execute_command(session.create_query_context(&BUILD_INFO).await?, sql).await
}

pub(super) async fn rows(session: &Arc<Session>, sql: &str) -> Result<usize> {
    let blocks: Vec<DataBlock> =
        execute_query(session.create_query_context(&BUILD_INFO).await?, sql)
            .await?
            .try_collect()
            .await?;
    Ok(blocks.iter().map(DataBlock::num_rows).sum())
}

pub(super) async fn setup() -> anyhow::Result<(TestFixture, Arc<Session>, LocalMetaService)> {
    // All query services must use the same metastore for cross-service share transactions.
    // The default empty endpoints create a separate embedded store per MetaStoreProvider.
    let meta = LocalMetaService::new_testing::<DatabendRuntime>("shared-stream").await?;
    let mut setup = EESetup::new();
    setup.config_mut().meta.endpoints = meta.get_cached_endpoints().await?;
    setup
        .config_mut()
        .query
        .common
        .internal_enable_sandbox_tenant = true;
    let fixture = TestFixture::setup_with_custom(setup).await?;
    for sql in [
        "CREATE DATABASE provider",
        "CREATE TABLE provider.t(a INT)",
        "CREATE CONNECTION share_conn STORAGE_TYPE = 'fs'",
        "CREATE SHARE s CONNECTION = share_conn",
        "GRANT USAGE ON DATABASE provider TO SHARE s",
        "GRANT SELECT ON TABLE provider.t TO SHARE s",
        "ALTER SHARE s ADD ACCOUNTS = stream_consumer",
    ] {
        fixture.execute_command(sql).await?;
    }
    let consumer = fixture.new_session_with_type(SessionType::Dummy).await?;
    consumer
        .get_settings()
        .set_setting("sandbox_tenant".to_string(), "stream_consumer".to_string())?;
    command(&consumer, "CREATE DATABASE local_db").await?;
    command(
        &consumer,
        &format!(
            "CREATE DATABASE shared FROM SHARE {}.s",
            fixture.default_tenant().tenant_name()
        ),
    )
    .await?;
    Ok((fixture, consumer, meta))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_cannot_write_provider() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (1)")
        .await?;
    let error = command(
        &consumer,
        "INSERT ALL INTO shared.t SELECT a FROM local_db.s",
    )
    .await
    .unwrap_err();
    assert!(error.message().contains("READ ONLY"), "{error}");
    let blocks: Vec<DataBlock> = fixture
        .execute_query("SELECT a FROM provider.t")
        .await?
        .try_collect()
        .await?;
    assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 1);
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_read_only_cache_and_navigation() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    command(&consumer, "CREATE TABLE local_db.sink(a INT)").await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (1)")
        .await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (2)")
        .await?;

    for forward_scan in [0, 1] {
        command(
            &consumer,
            &format!("SET enable_stream_batch_snapshot_forward_scan = {forward_scan}"),
        )
        .await?;
        for load_target_first in [false, true] {
            let ctx = consumer.create_query_context(&BUILD_INFO).await?;
            if load_target_first {
                ctx.get_table("default", "shared", "t").await?;
            }
            let table = ctx
                .resolve_data_source("default", "local_db", "s", None, Some(1))
                .await?;
            let source = StreamTable::try_from_table(table.as_ref())?
                .source_table(ctx.clone())
                .await?;
            assert!(source.is_read_only());
            assert!(
                FuseTable::try_from_table(source.as_ref())?
                    .check_mutable()
                    .is_err()
            );
            assert!(
                ctx.get_table("default", "shared", "t")
                    .await?
                    .is_read_only()
            );
            let error = execute_command(
                ctx,
                "INSERT ALL INTO local_db.sink INTO shared.t SELECT a FROM local_db.s WITH(MAX_BATCH_SIZE = 1)",
            )
            .await
            .unwrap_err();
            assert!(error.message().contains("READ ONLY"), "{error}");
        }
    }

    command(&consumer, "BEGIN").await?;
    assert_eq!(
        rows(
            &consumer,
            "SELECT a FROM local_db.s WITH(MAX_BATCH_SIZE = 1)"
        )
        .await?,
        1
    );
    let error = command(
        &consumer,
        "INSERT ALL INTO shared.t SELECT a FROM local_db.s WITH(MAX_BATCH_SIZE = 1)",
    )
    .await
    .unwrap_err();
    assert!(error.message().contains("READ ONLY"), "{error}");
    command(&consumer, "ROLLBACK").await?;

    let blocks: Vec<DataBlock> = fixture
        .execute_query("SELECT a FROM provider.t ORDER BY a")
        .await?
        .try_collect()
        .await?;
    let block = DataBlock::concat(&blocks)?;
    assert_eq!(block.num_rows(), 2);
    assert_eq!(
        block.get_by_offset(0).index(0),
        Some(ScalarRef::Number(NumberScalar::Int32(1)))
    );
    assert_eq!(
        block.get_by_offset(0).index(1),
        Some(ScalarRef::Number(NumberScalar::Int32(2)))
    );
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.sink").await?, 0);
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_table_authorization_is_resolved_per_statement() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    command(&consumer, "CREATE STREAM local_db.s2 ON TABLE shared.t").await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (1)")
        .await?;

    for first_read in ["SELECT a FROM shared.t", "SELECT a FROM local_db.s"] {
        for next_read in [
            "SELECT a FROM shared.t",
            "SELECT a FROM local_db.s2 WITH(MAX_BATCH_SIZE = 1)",
        ] {
            fixture
                .execute_command("GRANT SELECT ON TABLE provider.t TO SHARE s")
                .await?;
            command(&consumer, "BEGIN").await?;
            assert_eq!(rows(&consumer, first_read).await?, 1);
            fixture
                .execute_command("REVOKE SELECT ON TABLE provider.t FROM SHARE s")
                .await?;
            // Direct reads and an uncached stream must resolve the share again,
            // even if this transaction has already read the same source table.
            assert!(rows(&consumer, next_read).await.is_err());
            command(&consumer, "ROLLBACK").await?;
        }
    }

    for sql in [
        "GRANT SELECT ON TABLE provider.t TO SHARE s",
        "CREATE SHARE other CONNECTION = share_conn",
        "GRANT USAGE ON DATABASE provider TO SHARE other",
        "GRANT SELECT ON TABLE provider.t TO SHARE other",
        "ALTER SHARE other ADD ACCOUNTS = stream_consumer",
    ] {
        fixture.execute_command(sql).await?;
    }
    command(
        &consumer,
        &format!(
            "CREATE DATABASE other_shared FROM SHARE {}.other",
            fixture.default_tenant().tenant_name()
        ),
    )
    .await?;
    fixture
        .execute_command("REVOKE SELECT ON TABLE provider.t FROM SHARE other")
        .await?;
    command(&consumer, "BEGIN").await?;
    assert_eq!(rows(&consumer, "SELECT a FROM shared.t").await?, 1);
    // A provider table ID alone cannot identify an authorized consumer binding.
    assert!(
        rows(&consumer, "SELECT a FROM other_shared.t")
            .await
            .is_err()
    );
    command(&consumer, "ROLLBACK").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_create_and_independent_offsets() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let before = catalog
        .get_table(&fixture.default_tenant(), "provider", "t")
        .await?;
    let error = command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t")
        .await
        .unwrap_err();
    assert!(
        error
            .message()
            .contains("provider must enable change_tracking"),
        "{error}"
    );
    let after = catalog
        .get_table(&fixture.default_tenant(), "provider", "t")
        .await?;
    assert_eq!(before.get_table_info().ident, after.get_table_info().ident);

    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    let before = catalog
        .get_table(&fixture.default_tenant(), "provider", "t")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    command(
        &consumer,
        "CREATE STREAM local_db.s2 ON TABLE shared.t AT(STREAM => local_db.s)",
    )
    .await?;
    let after = catalog
        .get_table(&fixture.default_tenant(), "provider", "t")
        .await?;
    assert_eq!(before.get_table_info().ident, after.get_table_info().ident);
    let ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let stream = ctx.get_table("default", "local_db", "s").await?;
    assert!(stream.get_table_info().meta.storage_params.is_none());
    assert_ne!(
        stream.options().get(OPT_KEY_SOURCE_DATABASE_ID),
        stream.options().get(OPT_KEY_SOURCE_SHARED_DATABASE_ID)
    );
    assert!(
        command(&consumer, "CREATE STREAM shared.s ON TABLE shared.t")
            .await
            .is_err()
    );

    fixture
        .execute_command("INSERT INTO provider.t VALUES (1), (2)")
        .await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 2);
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 2);
    command(&consumer, "CREATE TABLE local_db.sink(a INT)").await?;
    command(
        &consumer,
        "INSERT INTO local_db.sink SELECT a FROM local_db.s",
    )
    .await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 0);
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s2").await?, 2);
    let boundary = chrono::Utc::now();
    fixture
        .execute_command("INSERT INTO provider.t VALUES (3)")
        .await?;
    command(&consumer, &format!(
        "CREATE STREAM local_db.history ON TABLE shared.t AT(TIMESTAMP => '{boundary:?}'::TIMESTAMP)"
    )).await?;
    assert_eq!(
        rows(&consumer, "SELECT a FROM local_db.history WHERE a = 3").await?,
        1
    );
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.history").await?, 1);
    fixture
        .execute_command("ALTER DATABASE provider RENAME TO provider_renamed")
        .await?;
    command(&consumer, "ALTER DATABASE shared RENAME TO shared_renamed").await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.history").await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_recreated_database() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (1)")
        .await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);

    let ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let catalog = ctx.get_catalog("default").await?;
    let stream = ctx.get_table("default", "local_db", "s").await?;
    let old_id = StreamTable::try_from_table(stream.as_ref())?
        .source_shared_database_id()?
        .unwrap();
    command(&consumer, "DROP DATABASE shared").await?;
    command(
        &consumer,
        &format!(
            "CREATE DATABASE shared FROM SHARE {}.s",
            fixture.default_tenant().tenant_name()
        ),
    )
    .await?;
    let database = catalog.get_database(&ctx.get_tenant(), "shared").await?;
    assert_ne!(old_id, database.get_db_info().database_id.db_id);
    assert_eq!(rows(&consumer, "SELECT a FROM shared.t").await?, 1);
    for sql in [
        "SELECT a FROM local_db.s",
        "SELECT a FROM local_db.s WITH(MAX_BATCH_SIZE = 1)",
    ] {
        let error = rows(&consumer, sql).await.unwrap_err();
        assert!(
            error.message().contains("Base database 'shared'"),
            "{error}"
        );
        assert!(error.message().contains("dropped"), "{error}");
    }
    command(
        &consumer,
        "CREATE OR REPLACE STREAM local_db.s ON TABLE shared.t",
    )
    .await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (2)")
        .await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_scan_plan_and_fresh_authorization() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (1)")
        .await?;
    let ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let shared_source = ctx.get_table("default", "shared", "t").await?;
    assert!(shared_source.get_table_info().is_shared());
    assert!(shared_source.is_read_only());
    assert!(!shared_source.plan_can_be_cached());
    assert!(!shared_source.result_can_be_cached());
    let table = ctx.get_table("default", "local_db", "s").await?;
    let stream = StreamTable::try_from_table(table.as_ref())?;
    let endpoint = stream.source_table(ctx.clone()).await?;
    let plan = table
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;
    let plan = serde_json::from_slice(&serde_json::to_vec(&plan)?)?;
    let restored = ctx.build_table_from_source_plan(&plan)?;
    let restored = StreamTable::try_from_table(restored.as_ref())?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (2)")
        .await?;
    // Rebuilding a scan in the same query must reuse the cached FuseTable endpoint.
    let cached_endpoint = restored.source_table(ctx.clone()).await?;
    assert_eq!(
        endpoint.get_table_info().ident,
        cached_endpoint.get_table_info().ident
    );
    assert_eq!(endpoint.options(), cached_endpoint.options());

    // A worker resolves the source again but reads only the planned partitions.
    let remote_ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let remote_table = remote_ctx.build_table_from_source_plan(&plan)?;
    let remote_source = StreamTable::try_from_table(remote_table.as_ref())?
        .source_table(remote_ctx.clone())
        .await?;
    assert!(remote_source.is_read_only());
    assert_eq!(
        remote_source.get_table_info().ident.table_id,
        endpoint.get_table_info().ident.table_id
    );
    let blocks: Vec<DataBlock> = remote_table
        .read_data_block_stream(remote_ctx.clone(), &plan)
        .await?
        .try_collect()
        .await?;
    let block = DataBlock::concat(&blocks)?;
    assert_eq!(block.num_rows(), 1);
    assert_eq!(
        block.get_by_offset(0).index(0),
        Some(ScalarRef::Number(NumberScalar::Int32(1)))
    );

    fixture
        .execute_command("REVOKE SELECT ON TABLE provider.t FROM SHARE s")
        .await?;
    // A fresh worker must resolve the share and reject the revoked grant.
    let fresh_ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let fresh_table = fresh_ctx.build_table_from_source_plan(&plan)?;
    assert!(
        StreamTable::try_from_table(fresh_table.as_ref())?
            .source_table(fresh_ctx)
            .await
            .is_err()
    );

    // A source already resolved in this query remains usable after revocation.
    restored.source_table(ctx.clone()).await?;
    stream.source_table(ctx.clone()).await?;
    shared_source
        .read_plan(ctx.clone(), None, None, false, true)
        .await?;
    // A new statement must resolve the share again and reject the revoked grant.
    assert!(rows(&consumer, "SELECT a FROM local_db.s").await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_transaction_snapshot_and_source_changes() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    command(&consumer, "CREATE STREAM local_db.s ON TABLE shared.t").await?;
    command(&consumer, "CREATE TABLE local_db.sink(a INT)").await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (1)")
        .await?;
    command(&consumer, "BEGIN").await?;
    command(
        &consumer,
        "INSERT INTO local_db.sink SELECT a FROM local_db.s",
    )
    .await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (2)")
        .await?;
    // A new QueryContext in the same transaction must restore the original shared endpoint.
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    command(&consumer, "COMMIT").await?;
    assert_eq!(
        rows(&consumer, "SELECT a FROM local_db.s WHERE a = 2").await?,
        1
    );

    command(&consumer, "BEGIN").await?;
    command(
        &consumer,
        "INSERT INTO local_db.sink SELECT a FROM local_db.s WITH(MAX_BATCH_SIZE = 1)",
    )
    .await?;
    fixture
        .execute_command("REVOKE SELECT ON TABLE provider.t FROM SHARE s")
        .await?;
    assert_eq!(
        rows(
            &consumer,
            "SELECT a FROM local_db.s WITH(MAX_BATCH_SIZE = 1)"
        )
        .await?,
        1
    );
    command(&consumer, "ROLLBACK").await?;
    assert!(rows(&consumer, "SELECT a FROM local_db.s").await.is_err());
    fixture
        .execute_command("GRANT SELECT ON TABLE provider.t TO SHARE s")
        .await?;
    assert_eq!(
        rows(&consumer, "SELECT a FROM local_db.s WHERE a = 2").await?,
        1
    );
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.sink").await?, 1);

    command(&consumer, "BEGIN").await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = false)")
        .await?;
    fixture
        .execute_command("ALTER TABLE provider.t SET OPTIONS(change_tracking = true)")
        .await?;
    // The transaction keeps its original endpoint and tracking metadata.
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    command(&consumer, "ROLLBACK").await?;
    let error = rows(&consumer, "SELECT a FROM local_db.s")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Change tracking has been missing")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_stream_transaction_provider_storage() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    // This root is separate from the fixture's default storage used by the consumer.
    let provider_root = tempfile::tempdir()?;
    let storage = StorageParams::Fs(StorageFsConfig {
        root: provider_root.path().to_str().unwrap().to_string(),
    });
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let template = catalog
        .get_table(&fixture.default_tenant(), "provider", "t")
        .await?;
    let mut table_meta = template.get_table_info().meta.clone();
    table_meta.storage_params = Some(storage.clone());
    catalog
        .create_table(CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant: fixture.default_tenant(),
                db_name: "provider".to_string(),
                table_name: "external".to_string(),
            },
            table_meta,
            source_table_option: None,
            as_dropped: false,
            materialized_view: None,
            table_properties: None,
            table_partition: None,
        })
        .await?;
    fixture
        .execute_command("ALTER TABLE provider.external SET OPTIONS(change_tracking = true)")
        .await?;
    fixture
        .execute_command("GRANT SELECT ON TABLE provider.external TO SHARE s")
        .await?;
    command(
        &consumer,
        "CREATE STREAM local_db.s ON TABLE shared.external",
    )
    .await?;
    fixture
        .execute_command("INSERT INTO provider.external VALUES (1)")
        .await?;
    // Execution retains the provider storage, but consumer-facing metadata
    // must not expose its location or credentials.
    let blocks: Vec<DataBlock> = execute_query(
        consumer.create_query_context(&BUILD_INFO).await?,
        "SHOW CREATE TABLE shared.external",
    )
    .await?
    .try_collect()
    .await?;
    let block = DataBlock::concat(&blocks)?;
    let Some(ScalarRef::String(create_sql)) = block.get_by_offset(1).index(0) else {
        panic!("SHOW CREATE TABLE must return the table definition");
    };
    assert!(!create_sql.contains(provider_root.path().to_str().unwrap()));
    let blocks: Vec<DataBlock> = execute_query(
        consumer.create_query_context(&BUILD_INFO).await?,
        "SELECT is_external, storage_param FROM system.tables WHERE database = 'shared' AND name = 'external'",
    )
    .await?
    .try_collect()
    .await?;
    let block = DataBlock::concat(&blocks)?;
    assert_eq!(block.num_rows(), 1);
    assert_eq!(
        block.get_by_offset(0).index(0),
        Some(ScalarRef::Boolean(false))
    );
    assert_eq!(block.get_by_offset(1).index(0), Some(ScalarRef::String("")));
    command(&consumer, "BEGIN").await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    fixture
        .execute_command("INSERT INTO provider.external VALUES (2)")
        .await?;
    // A new statement restores the original endpoint from the provider's storage root.
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 1);
    let ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let table = ctx.get_table("default", "local_db", "s").await?;
    let source = StreamTable::try_from_table(table.as_ref())?
        .source_table(ctx)
        .await?;
    assert_eq!(
        source.get_table_info().meta.storage_params.as_ref(),
        Some(&storage)
    );
    command(&consumer, "ROLLBACK").await?;
    assert_eq!(rows(&consumer, "SELECT a FROM local_db.s").await?, 2);
    Ok(())
}
