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
use databend_common_management::meta_read_cache::MetaReadCache;
use databend_common_management::meta_read_cache::RangeStats;
use databend_common_management::*;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::UpsertKV;
use databend_meta_runtime::DatabendRuntime;
use fastrace::func_name;

const LONG: Duration = Duration::from_secs(3600);
const USER_KEY: &str = "__fd_users/t1/u1";
const SETTING_DIR: &str = "__fd_settings/t1/";

fn tenant() -> Tenant {
    Tenant::new_or_err("t1", func_name!()).unwrap()
}

fn stats(cache: &MetaReadCache, name: &str) -> RangeStats {
    cache
        .stats()
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap()
        .1
}

fn max_threads(v: u64) -> UserSetting {
    UserSetting::create("max_threads", UserSettingValue::UInt64(v))
}

struct Setup {
    /// Writes here stand for another node: they do not go through the cache.
    meta: MetaStore,
    cache: Arc<MetaReadCache>,
    reading: Arc<CachedKvApi>,
    invalidating: Arc<CachedKvApi>,
}

async fn setup(ttl: Duration) -> Setup {
    let meta = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let cache = MetaReadCache::create(&tenant());
    cache.set_ttl(ttl);
    let inner: Arc<dyn KVApi<Error = MetaError>> = Arc::new(meta.clone());
    Setup {
        reading: Arc::new(CachedKvApi::reading(inner.clone(), cache.clone())),
        invalidating: Arc::new(CachedKvApi::invalidating(inner, cache.clone())),
        meta,
        cache,
    }
}

/// With a zero TTL, the initial value, every read goes to meta-service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_zero_ttl_reads_meta_service() -> anyhow::Result<()> {
    let s = setup(Duration::ZERO).await;
    let reader = SettingMgr::create(s.reading.clone(), &tenant());
    let other = SettingMgr::create(Arc::new(s.meta.clone()), &tenant());

    for i in 1..=3 {
        other.set_setting(max_threads(i)).await?;
        assert_eq!(reader.get_settings().await?, vec![max_threads(i)]);
    }
    assert_eq!(stats(&s.cache, "setting"), RangeStats::default());
    Ok(())
}

/// A result is kept for the TTL: a change by another node is not seen until then, a write by
/// this process is seen at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_kept_for_ttl_and_dropped_by_local_write() -> anyhow::Result<()> {
    let s = setup(Duration::from_millis(300)).await;
    let reader = SettingMgr::create(s.reading.clone(), &tenant());
    let writer = SettingMgr::create(s.invalidating.clone(), &tenant());
    let other = SettingMgr::create(Arc::new(s.meta.clone()), &tenant());

    // No global setting is kept too: it means every setting has its default.
    assert!(reader.get_settings().await?.is_empty());
    assert!(reader.get_settings().await?.is_empty());
    assert_eq!(stats(&s.cache, "setting"), RangeStats { hit: 1, miss: 1 });

    // Read-your-writes.
    for i in 1..=5 {
        writer.set_setting(max_threads(i)).await?;
        assert_eq!(reader.get_settings().await?, vec![max_threads(i)]);
    }

    // Another node: the kept result answers until it is one TTL old.
    other.set_setting(max_threads(99)).await?;
    assert_eq!(reader.get_settings().await?, vec![max_threads(5)]);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(reader.get_settings().await?, vec![max_threads(99)]);

    // Dropping a setting is a write too.
    writer.try_drop_setting("max_threads").await?;
    assert!(reader.get_settings().await?.is_empty());
    Ok(())
}

/// Setting the TTL to zero stops answering from memory at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_zero_ttl_takes_effect_at_once() -> anyhow::Result<()> {
    let s = setup(LONG).await;
    s.meta.upsert_kv(UpsertKV::update(USER_KEY, b"v1")).await?;
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v1");

    s.meta.upsert_kv(UpsertKV::update(USER_KEY, b"v2")).await?;
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v1");

    s.cache.set_ttl(Duration::ZERO);
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v2");
    Ok(())
}

/// An absent user is not kept, so a user created by another node is found at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_absent_user_is_not_kept() -> anyhow::Result<()> {
    let s = setup(LONG).await;
    assert!(s.reading.get_kv(USER_KEY).await?.is_none());

    s.meta.upsert_kv(UpsertKV::update(USER_KEY, b"v1")).await?;
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v1");
    assert_eq!(stats(&s.cache, "user"), RangeStats { hit: 0, miss: 2 });

    // Kept from now on.
    s.meta.upsert_kv(UpsertKV::delete(USER_KEY)).await?;
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v1");
    Ok(())
}

/// The handle that does not answer reads still drops what the reading one kept, and a key
/// outside the cached ranges is never kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_writes_and_other_keys() -> anyhow::Result<()> {
    let s = setup(LONG).await;
    s.meta.upsert_kv(UpsertKV::update(USER_KEY, b"v1")).await?;
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v1");

    // Its own reads see meta-service, also while a result is kept.
    s.meta.upsert_kv(UpsertKV::update(USER_KEY, b"v2")).await?;
    assert_eq!(s.invalidating.get_kv(USER_KEY).await?.unwrap().data, b"v2");
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v1");

    s.invalidating
        .upsert_kv(UpsertKV::update(USER_KEY, b"v3"))
        .await?;
    assert_eq!(s.reading.get_kv(USER_KEY).await?.unwrap().data, b"v3");

    let other = "__fd_roles/t1/r";
    s.meta.upsert_kv(UpsertKV::update(other, b"v1")).await?;
    assert_eq!(s.reading.get_kv(other).await?.unwrap().data, b"v1");
    s.meta.upsert_kv(UpsertKV::update(other, b"v2")).await?;
    assert_eq!(s.reading.get_kv(other).await?.unwrap().data, b"v2");
    Ok(())
}

/// A kept key with an expiry is not returned after it expired, as a direct read would not; a
/// list with a limit is not the whole range and is not kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_expired_keys_and_limited_lists() -> anyhow::Result<()> {
    let s = setup(LONG).await;
    let list = |limit: Option<u64>| {
        let reading = s.reading.clone();
        async move {
            let opts = match limit {
                Some(n) => ListOptions::limited(SETTING_DIR, n),
                None => ListOptions::unlimited(SETTING_DIR),
            };
            reading
                .list_kv_collect(opts)
                .await
                .map(|kvs| kvs.into_iter().map(|(k, _)| k).collect::<Vec<_>>())
        }
    };
    let ttl = Duration::from_millis(300);
    let (a, b) = (format!("{SETTING_DIR}a"), format!("{SETTING_DIR}b"));
    s.meta
        .upsert_kv(UpsertKV::update(&a, b"1").with_ttl(ttl))
        .await?;
    s.meta.upsert_kv(UpsertKV::update(&b, b"2")).await?;

    assert_eq!(list(None).await?, vec![a.clone(), b.clone()]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(list(None).await?, vec![b.clone()]);
    assert_eq!(stats(&s.cache, "setting"), RangeStats { hit: 1, miss: 1 });

    let c = format!("{SETTING_DIR}c");
    s.meta.upsert_kv(UpsertKV::update(&c, b"3")).await?;
    assert_eq!(list(Some(10)).await?, vec![b.clone(), c]);
    assert_eq!(list(None).await?, vec![b]);
    Ok(())
}
