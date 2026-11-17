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

//! `WatchedPrefix`: which reads the watched copy answers, and which go to meta-service.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::Change;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::protobuf::StreamItem;
use databend_meta_plugin_cache::WatchedPrefix;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use test_harness::test;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio::time::timeout;

/// A meta-service that has no keys and counts the reads that reach it.
#[derive(Default)]
struct CountingKv {
    reads: AtomicUsize,
}

impl CountingKv {
    fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl KVApi for CountingKv {
    type Error = MetaError;

    async fn upsert_kv(&self, _req: UpsertKV) -> Result<Change<Vec<u8>>, MetaError> {
        unreachable!("WatchedPrefix only reads")
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, MetaError>>,
    ) -> Result<KVStream<MetaError>, MetaError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(keys.map_ok(|key| StreamItem::new(key, None)).boxed())
    }

    async fn list_kv(&self, _opts: ListOptions<'_, str>) -> Result<KVStream<MetaError>, MetaError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(stream::empty().boxed())
    }

    async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, MetaError> {
        unreachable!("WatchedPrefix only reads")
    }
}

/// Wait until the copy answers `key`, with the value `want`.
async fn wait_for(watched: &WatchedPrefix, key: &str, want: &[u8]) -> anyhow::Result<()> {
    let kv = CountingKv::default();
    for _ in 0..100 {
        let got = watched.mget_kv(&kv, &[key.to_string()]).await?;
        if got[0].as_ref().map(|v| v.data.as_slice()) == Some(want) {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("the watched copy did not answer {key} = {want:?}")
}

fn data(values: Vec<Option<SeqV>>) -> Vec<Option<Vec<u8>>> {
    values.into_iter().map(|v| v.map(|v| v.data)).collect()
}

#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_watched_prefix_reads() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0]).await?;
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        vec![tcs[0].config.grpc.api_address().unwrap()],
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    client.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;
    client.upsert_kv(UpsertKV::update("pp/d/x", b"x")).await?;
    client
        .upsert_kv(UpsertKV::update("ppp/a", b"other"))
        .await?;

    // The trailing `/` is ignored.
    let watched = WatchedPrefix::new(client.clone(), "pp/", "test-watched").await;
    assert_eq!(watched.prefix(), "pp");
    wait_for(&watched, "pp/a", b"a").await?;

    // A ready copy answers every key under the prefix, absent ones included.
    let kv = CountingKv::default();
    let got = watched
        .mget_kv(&kv, &["pp/a".to_string(), "pp/missing".to_string()])
        .await?;
    assert_eq!(data(got), vec![Some(b"a".to_vec()), None]);
    let dir = watched.list_kv(&kv, "pp/d/").await?;
    assert_eq!(dir.len(), 1);
    assert_eq!(
        (dir[0].0.as_str(), dir[0].1.data.as_slice()),
        ("pp/d/x", &b"x"[..])
    );
    let all = watched.list_kv(&kv, "pp").await?;
    assert_eq!(
        all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        vec!["pp/a", "pp/d/x"]
    );
    // Only one trailing `/` is ignored: `pp/d//` lists the keys `pp/d//...`, as meta-service does.
    assert!(watched.list_kv(&kv, "pp/d//").await?.is_empty());
    assert_eq!(kv.reads(), 0);

    // Keys outside the prefix, `ppp/` included, go to meta-service, all in one read.
    let got = watched
        .mget_kv(&kv, &["pp/a".to_string(), "ppp/a".to_string()])
        .await?;
    assert_eq!(data(got), vec![None, None]);
    assert!(watched.list_kv(&kv, "ppp").await?.is_empty());
    assert_eq!(kv.reads(), 2);

    // Changes reach the copy by the watch.
    client.upsert_kv(UpsertKV::update("pp/a", b"a2")).await?;
    wait_for(&watched, "pp/a", b"a2").await?;
    Ok(())
}

/// A key whose TTL has passed is absent from the copy, though no write has made meta-service
/// remove it and tell the watch.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_watched_prefix_hides_expired_keys() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0]).await?;
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        vec![tcs[0].config.grpc.api_address().unwrap()],
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    let watched = WatchedPrefix::new(client.clone(), "pp", "test-expired").await;
    client.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;
    client
        .upsert_kv(UpsertKV::update("pp/t", b"t").with_ttl(Duration::from_secs(3)))
        .await?;
    wait_for(&watched, "pp/a", b"a").await?;
    wait_for(&watched, "pp/t", b"t").await?;

    sleep(Duration::from_secs(4)).await;

    let kv = CountingKv::default();
    let got = watched
        .mget_kv(&kv, &["pp/a".to_string(), "pp/t".to_string()])
        .await?;
    assert_eq!(data(got), vec![Some(b"a".to_vec()), None]);
    let all = watched.list_kv(&kv, "pp").await?;
    assert_eq!(
        all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        vec!["pp/a"]
    );
    assert_eq!(kv.reads(), 0);
    Ok(())
}

/// A copy that never becomes ready sends every read to meta-service without waiting for it.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_watched_prefix_not_ready_does_not_block() -> anyhow::Result<()> {
    // TCP connects, but nothing answers the handshake: the watch stays initializing.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        vec![listener.local_addr()?.to_string()],
        "root",
        "xxx",
        Some(Duration::from_secs(60)),
        Some(Duration::from_secs(60)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    let watched = WatchedPrefix::new(client, "pp", "test-not-ready").await;

    let kv = CountingKv::default();
    let readers = (0..16).map(|_| async {
        let got = watched.mget_kv(&kv, &["pp/a".to_string()]).await?;
        let dir = watched.list_kv(&kv, "pp").await?;
        Ok::<_, MetaError>((got, dir))
    });
    for result in timeout(Duration::from_secs(5), join_all(readers)).await? {
        let (got, dir) = result?;
        assert_eq!(data(got), vec![None]);
        assert!(dir.is_empty());
    }
    assert_eq!(kv.reads(), 32);
    Ok(())
}
