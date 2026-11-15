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

use std::sync::Arc;
use std::time::Duration;

use databend_common_management::meta_read_cache::CachedKvApi;
use databend_common_management::meta_read_cache::CachedPrefix;
use databend_common_management::meta_read_cache::MetaReadCache;
use databend_common_management::meta_read_cache::PrefixStats;
use databend_common_management::*;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::Change;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_runtime::DatabendRuntime;
use fastrace::func_name;
use futures::stream::BoxStream;

const ALL: [CachedPrefix; 3] = [
    CachedPrefix::User,
    CachedPrefix::Setting,
    CachedPrefix::Cluster,
];

fn tenant() -> Tenant {
    Tenant::new_or_err("t1", func_name!()).unwrap()
}

fn stats(cache: &MetaReadCache, name: &str) -> PrefixStats {
    cache
        .stats()
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap()
        .1
}

async fn wait_serving(cache: &MetaReadCache, name: &str) {
    for _ in 0..200 {
        if cache.is_serving(name) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{name} is not serving");
}

async fn setup(
    interval: Duration,
) -> anyhow::Result<(
    MetaStore,
    Arc<MetaReadCache>,
    Arc<dyn KVApi<Error = MetaError>>,
)> {
    let meta = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let cache = MetaReadCache::create(meta.clone(), &tenant(), &ALL, interval).await?;
    let inner: Arc<dyn KVApi<Error = MetaError>> = Arc::new(meta.clone());
    Ok((meta, cache, inner))
}

/// A setting written through the fencing client is visible at once to a reader of the same
/// process; a write by another process arrives through the watch stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_your_writes_and_remote_writes() -> anyhow::Result<()> {
    let (meta, cache, inner) = setup(Duration::from_secs(3600)).await?;
    let writer = SettingMgr::create(
        Arc::new(CachedKvApi::fencing(inner.clone(), cache.clone())),
        &tenant(),
    );
    let reader = SettingMgr::create(
        Arc::new(CachedKvApi::reading(inner, cache.clone())),
        &tenant(),
    );
    wait_serving(&cache, "setting").await;

    // No global setting: answered from memory, as "all defaults".
    assert!(reader.get_settings().await?.is_empty());
    assert_eq!(stats(&cache, "setting").hit, 1);

    for i in 1..=20u64 {
        let s = UserSetting::create("max_threads", UserSettingValue::UInt64(i));
        writer.set_setting(s.clone()).await?;
        assert_eq!(
            reader.get_settings().await?,
            vec![s],
            "read-your-writes at {i}"
        );
    }
    let st = stats(&cache, "setting");
    assert_eq!(st.hit + st.fallback_fence, 21, "{st:?}");

    // Another process writes directly to meta-service.
    let other = SettingMgr::create(Arc::new(meta.clone()), &tenant());
    let s = UserSetting::create("max_threads", UserSettingValue::UInt64(99));
    other.set_setting(s.clone()).await?;
    let mut seen = false;
    for _ in 0..200 {
        if reader.get_settings().await? == vec![s.clone()] {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(seen, "remote write not delivered");
    Ok(())
}

/// An absent user is never answered from memory, so a user created elsewhere is found at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_absent_user_falls_back() -> anyhow::Result<()> {
    let (meta, cache, inner) = setup(Duration::from_secs(3600)).await?;
    let reading = CachedKvApi::reading(inner, cache.clone());
    wait_serving(&cache, "user").await;

    let key = "__fd_users/t1/u1";
    assert!(reading.get_kv(key).await?.is_none());
    assert_eq!(stats(&cache, "user").fallback_absent, 1);

    meta.upsert_kv(UpsertKV::update(key, b"v1")).await?;
    assert_eq!(reading.get_kv(key).await?.unwrap().data, b"v1".to_vec());
    Ok(())
}

/// Expired cluster nodes are filtered like a direct read does, even before meta-service deletes
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_expired_nodes_are_not_served() -> anyhow::Result<()> {
    let (meta, cache, inner) = setup(Duration::from_secs(3600)).await?;
    let reading = CachedKvApi::reading(inner, cache.clone());
    wait_serving(&cache, "cluster").await;

    let dir = "__fd_clusters_v6/t1/online_clusters/wh/c/";
    let ttl = Duration::from_millis(500);
    meta.upsert_kv(UpsertKV::update(format!("{dir}n1"), b"n1").with_ttl(ttl))
        .await?;
    meta.upsert_kv(UpsertKV::update(format!("{dir}n2"), b"n2"))
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        reading
            .list_kv_collect(ListOptions::unlimited(dir))
            .await?
            .len(),
        2
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    let live = reading.list_kv_collect(ListOptions::unlimited(dir)).await?;
    assert_eq!(
        live.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        vec![format!("{dir}n2")]
    );
    Ok(())
}

/// Reports a write with a seq the watch stream will never deliver, as a stalled stream would.
struct NeverDelivered(MetaStore);

#[async_trait::async_trait]
impl KVApi for NeverDelivered {
    type Error = MetaError;

    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        let _ = req;
        Ok(Change::new(None, Some(SeqV::new(1_000_000, b"x".to_vec()))))
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        self.0.get_many_kv(keys).await
    }

    async fn list_kv(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        self.0.list_kv(opts).await
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        self.0.transaction(txn).await
    }
}

/// A local write not delivered within one verify interval stops serving from memory and
/// re-fetches the stream; reads keep working from meta-service meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_undelivered_write_stops_serving() -> anyhow::Result<()> {
    let (meta, cache, inner) = setup(Duration::from_millis(200)).await?;
    let reading = CachedKvApi::reading(inner, cache.clone());
    wait_serving(&cache, "setting").await;

    let key = "__fd_settings/t1/max_threads";
    meta.upsert_kv(UpsertKV::update(key, b"real")).await?;
    let fencing = CachedKvApi::fencing(Arc::new(NeverDelivered(meta.clone())), cache.clone());
    fencing.upsert_kv(UpsertKV::update(key, b"x")).await?;

    // Fenced: falls back and returns what meta-service has.
    assert_eq!(reading.get_kv(key).await?.unwrap().data, b"real".to_vec());
    assert!(stats(&cache, "setting").fallback_fence >= 1);

    tokio::time::sleep(Duration::from_millis(700)).await;
    let st = stats(&cache, "setting");
    assert!(st.resync >= 1, "{st:?}");
    assert!(!cache.is_serving("setting"));
    assert_eq!(reading.get_kv(key).await?.unwrap().data, b"real".to_vec());
    assert!(stats(&cache, "setting").fallback_suspect >= 1);
    Ok(())
}
