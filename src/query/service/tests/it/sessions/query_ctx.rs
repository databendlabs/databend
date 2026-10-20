// Copyright 2022 Datafuse Labs.
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

use std::time::Duration;

use anyhow::Context;
use databend_common_catalog::lock::LockTableOption;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::StructKey;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_s3() -> anyhow::Result<()> {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/bucket"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = ConfigBuilder::create().config();
    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "bucket".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        disable_credential_loader: true,
        ..Default::default()
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;

    let _ = ctx.get_application_level_data_operator()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_storage_accessor_fs() -> anyhow::Result<()> {
    let mut conf = ConfigBuilder::create().config();
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: "/tmp".to_string(),
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;
    let _ = ctx.get_application_level_data_operator()?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_created_time_propagation() -> anyhow::Result<()> {
    use std::time::Duration;
    use std::time::SystemTime;

    use databend_common_catalog::session_type::SessionType;
    use databend_common_version::BUILD_INFO;
    use databend_query::schedulers::QueryFragmentsActions;
    use databend_query::servers::flight::v1::packets::QueryEnv;
    use databend_query::sessions::QueryContext;
    use databend_query::sessions::TableContextCluster;
    use databend_query::sessions::TableContextQueryIdentity;
    use databend_query::sessions::TableContextSettings;

    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::MySQL).await?;

    let ctx = session.create_query_context(&BUILD_INFO).await?;
    assert_eq!(
        ctx.get_function_context()?.now,
        chrono::DateTime::<chrono::Utc>::from(ctx.get_query_created_time())
    );
    assert_eq!(
        session.process_info().created_time,
        ctx.get_query_created_time()
    );
    let cluster = ctx.get_cluster();
    drop(ctx);

    // Use an explicit historical timestamp to catch worker-local clock sampling.
    let query_created_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let ctx = session.create_query_context_with_cluster(
        cluster.clone(),
        &BUILD_INFO,
        Some(query_created_time),
    )?;
    assert_eq!(ctx.get_query_created_time(), query_created_time);
    let expected_now: chrono::DateTime<chrono::Utc> = query_created_time.into();
    assert_eq!(ctx.get_function_context()?.now, expected_now);

    let env = QueryFragmentsActions::create(ctx.clone()).get_query_env()?;
    assert_eq!(env.query_created_time, query_created_time);
    let serialized = serde_json::to_value(&env)?;
    let received: QueryEnv = serde_json::from_value(serialized)?;
    let before_worker = SystemTime::now();
    let worker = received.create_query_ctx().await?;
    assert_eq!(worker.get_id(), ctx.get_id());
    assert_eq!(worker.get_query_created_time(), query_created_time);
    assert_eq!(worker.get_function_context()?.now, expected_now);
    let worker_info = worker.get_current_session().process_info();
    assert!(worker_info.created_time >= before_worker);
    assert_ne!(worker_info.created_time, query_created_time);
    let derived = QueryContext::create_from(&worker);
    assert_eq!(derived.get_query_created_time(), query_created_time);
    assert_eq!(derived.get_function_context()?.now, expected_now);
    assert_eq!(worker.get_function_context()?.now, expected_now);

    drop(ctx);
    let next_time = query_created_time + Duration::from_secs(60);
    let next = session.create_query_context_with_cluster(cluster, &BUILD_INFO, Some(next_time))?;
    assert_eq!(next.get_query_created_time(), next_time);
    assert_eq!(
        next.get_function_context()?.now,
        chrono::DateTime::<chrono::Utc>::from(next_time)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_external_timeout_releases_table_lock_revision() -> anyhow::Result<()> {
    let ttl = Duration::from_secs(3);
    let cleanup_timeout = ttl * 2 + Duration::from_secs(1);
    // Catalog lists revisions while UserApiProvider watches them. Both must connect to the
    // same Meta; the default fixture gives each a separate embedded Meta instance.
    let meta = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let mut config = ConfigBuilder::create().build();
    config.meta.endpoints = meta.inner().get_cached_endpoints().await?;
    config.meta.auto_sync_interval = 0;
    let fixture = TestFixture::setup_with_config(&config).await?;
    let settings = fixture.default_session().get_settings();
    settings.set_setting(
        "table_lock_expire_secs".to_string(),
        ttl.as_secs().to_string(),
    )?;
    settings.set_setting("acquire_lock_timeout".to_string(), "60".to_string())?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let catalog_name = fixture.default_catalog_name();
    let owner_ctx = fixture.new_query_ctx().await?;
    let catalog = owner_ctx.get_catalog(&catalog_name).await?;
    let table_id = fixture.latest_default_table().await?.get_id();
    let lock_key = LockKey::Table {
        tenant: fixture.default_tenant(),
        table_id,
    };
    let wait_for_revisions = |count| {
        let catalog = &catalog;
        let lock_key = lock_key.clone();
        async move {
            tokio::time::timeout(cleanup_timeout, async {
                loop {
                    let revisions = catalog
                        .list_lock_revisions(ListLockRevReq::new(lock_key.clone()))
                        .await?;
                    if revisions.len() == count {
                        return Ok::<_, anyhow::Error>(revisions);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .with_context(|| format!("expected {count} lock revisions"))?
        }
    };
    let owner = owner_ctx
        .acquire_table_lock_by_id(&catalog_name, table_id, &LockTableOption::LockWithRetry)
        .await?
        .context("the first writer must acquire the lock")?;
    let owner_revision = wait_for_revisions(1).await?[0].0;
    let waiter_ctx = fixture.new_query_ctx().await?;
    let mut acquisition = Box::pin(waiter_ctx.clone().acquire_table_lock_by_id(
        &catalog_name,
        table_id,
        &LockTableOption::LockWithRetry,
    ));
    // Poll on the initialized test thread: Databend's test globals are thread-name scoped.
    let queued = tokio::select! {
        result = &mut acquisition => {
            match result {
                Ok(_) => anyhow::bail!("the second writer acquired an already held lock"),
                Err(error) => anyhow::bail!("waiting failed before cancellation: {error}"),
            }
        }
        result = wait_for_revisions(2) => result?,
    };
    let waiting_revision = queued
        .iter()
        .find(|(_, meta)| meta.acquired_on.is_none())
        .context("the second writer must have a queued revision")?
        .0;
    let key = lock_key.gen_v2_key(waiting_revision).to_string_key();
    let initial_seq = meta
        .get_kv(&key)
        .await?
        .context("queued lease must exist")?
        .seq;
    let renewal = async {
        loop {
            let lease = meta.get_kv(&key).await?.context("queued lease expired")?;
            if lease.seq > initial_seq {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    // Observing Create alone can race its reply. A renewal proves the keeper has started.
    tokio::select! {
        result = &mut acquisition => {
            match result {
                Ok(_) => anyhow::bail!("the second writer acquired an already held lock"),
                Err(error) => anyhow::bail!("waiting failed before cancellation: {error}"),
            }
        }
        result = tokio::time::timeout(ttl * 2, renewal) => {
            result.context("the queued lease must renew before cancellation")??;
        }
    }
    // This owns and drops the future, well before the 60s internal acquire timeout.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), acquisition)
            .await
            .is_err()
    );
    let cleanup = wait_for_revisions(1).await;
    if cleanup.is_err() {
        // If the regression returns, do not leave its detached keeper renewing forever after
        // the test globals are torn down. Delete only the abandoned waiter and let it stop.
        catalog
            .delete_lock_revision(DeleteLockRevReq::new(lock_key.clone(), waiting_revision))
            .await?;
        tokio::time::sleep(ttl).await;
    }
    let remaining = cleanup.context("cancelled acquisition kept renewing its revision")?;
    assert_eq!(remaining[0].0, owner_revision, "the owner must stay locked");
    assert!(remaining[0].1.acquired_on.is_some());
    drop(owner);
    wait_for_revisions(0).await?;
    let guard = waiter_ctx
        .acquire_table_lock_by_id(&catalog_name, table_id, &LockTableOption::LockNoRetry)
        .await?
        .context("a subsequent writer must acquire the table lock")?;
    drop(guard);
    wait_for_revisions(0).await?;
    Ok(())
}
