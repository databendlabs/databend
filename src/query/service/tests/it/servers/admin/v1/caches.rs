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

use databend_common_config::InnerConfig;
use databend_query::servers::admin::AdminService;
use databend_query::sessions::SessionManager;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::PartStatistics;
use databend_storages_common_cache::Partitions;
use databend_storages_common_cache::kinds::table;
use http::Method;
use http::StatusCode;
use http::Uri;
use http::header;
use poem::Endpoint;
use poem::IntoResponse;
use poem::Request;
use serde_json::Value;
use serde_json::json;

async fn get_caches(config: &InnerConfig) -> anyhow::Result<Vec<Value>> {
    let router = AdminService::create(config).build_router();
    let response = router
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/caches"))
                .method(Method::GET)
                .finish(),
        )
        .await?
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE]
            .to_str()?
            .split(';')
            .next(),
        Some("application/json")
    );
    Ok(serde_json::from_slice(
        &response.into_body().into_vec().await?,
    )?)
}

fn cache<'a>(rows: &'a [Value], name: &str) -> &'a Value {
    rows.iter().find(|row| row["name"] == name).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_caches_legacy_snapshot_and_refresh() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.cache.shared_memory_enabled = false;
    config.cache.enable_table_meta_cache = true;
    config.cache.table_prune_partitions_count = 4;
    let _fixture = TestFixture::setup_with_config(&config).await?;
    let sessions = SessionManager::instance();
    let session_count = sessions.processes_info().len();
    let prune = CacheManager::instance().cache::<table::PruneResult>();

    let rows = get_caches(&config).await?;
    let before = cache(&rows, "memory_cache_prune_partitions");
    assert_eq!(before["node"], config.query.node_id);
    assert_eq!(before["pool"], "meta");
    assert_eq!(before["is_pool"], false);
    assert_eq!(before["enabled"], true);
    assert_eq!(before["capacity"], 4);
    assert_eq!(before["unit"], "count");
    assert_eq!(before["num_items"], 0);
    let access = before["access"].as_u64().unwrap();
    let hit = before["hit"].as_u64().unwrap();
    let miss = before["miss"].as_u64().unwrap();

    drop(prune.put(
        "admin-caches-test",
        (PartStatistics::default(), Partitions::default()),
    ));
    assert!(prune.lookup("admin-caches-test").is_some());
    assert!(prune.lookup("admin-caches-missing").is_none());

    let rows = get_caches(&config).await?;
    let after = cache(&rows, "memory_cache_prune_partitions");
    assert_eq!(after["num_items"], 1);
    assert_eq!(after["access"], access + 2);
    assert_eq!(after["hit"], hit + 1);
    assert_eq!(after["miss"], miss + 1);
    assert_eq!(
        cache(&get_caches(&config).await?, "memory_cache_prune_partitions"),
        after
    );
    assert_eq!(sessions.processes_info().len(), session_count);
    let status = sessions.get_current_session_status();
    assert_eq!(status.running_queries_count, 0);
    assert!(status.last_query_started_at.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_caches_includes_disabled_caches() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.cache.shared_memory_enabled = false;
    config.cache.enable_table_meta_cache = false;
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let rows = get_caches(&config).await?;
    let snapshot = cache(&rows, "memory_cache_table_snapshot");
    assert_eq!(snapshot["enabled"], false);
    assert_eq!(snapshot["num_items"], 0);
    assert_eq!(snapshot["size"], 0);
    assert_eq!(snapshot["in_cache_size"], 0);
    assert_eq!(snapshot["capacity"], 0);
    assert_eq!(snapshot["unit"], "count");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_caches_shared_pools_and_nullable_capacity() -> anyhow::Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.query.common.max_memory_limit_enabled = true;
    config.query.common.max_server_memory_usage = 1 << 30;
    config.cache.shared_memory_enabled = true;
    config.cache.meta_max_ratio = 10;
    config.cache.index_max_ratio = 20;
    config.cache.data_max_ratio = 0;
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let rows = get_caches(&config).await?;
    assert!(rows.iter().all(|row| row["node"] == config.query.node_id));
    let snapshot = cache(&rows, "memory_cache_table_snapshot");
    assert_eq!(snapshot["capacity"], Value::Null);
    assert_eq!(snapshot["unit"], "bytes");
    assert_eq!(snapshot["is_pool"], false);
    assert_eq!(snapshot["enabled"], true);
    assert_eq!(snapshot["pool"], "meta");

    let meta = cache(&rows, "__pool:meta");
    assert_eq!(meta["is_pool"], true);
    assert_eq!(meta["pool"], "meta");
    assert_eq!(meta["unit"], "bytes");
    assert_eq!(meta["capacity"], json!((1_u64 << 30) * 10 / 100));
    assert_eq!(meta["enabled"], true);
    let data = cache(&rows, "__pool:data");
    assert_eq!(data["is_pool"], true);
    assert_eq!(data["capacity"], 0);
    assert_eq!(data["enabled"], false);
    Ok(())
}
