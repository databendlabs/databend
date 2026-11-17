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

//! `WatchedPrefix`: which reads the watched copy answers, and which it leaves to meta-service.

use std::time::Duration;

use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::UpsertKV;
use databend_meta_plugin_cache::WatchedPrefix;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use test_harness::test;
use tokio::net::TcpListener;
use tokio::time::sleep;

/// Wait until the copy answers `key`, with the value `want`.
async fn wait_for(watched: &WatchedPrefix, key: &str, want: &[u8]) -> anyhow::Result<()> {
    for _ in 0..100 {
        if data(watched.try_get_ready(key)).as_deref() == Some(want) {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("the watched copy did not answer {key} = {want:?}")
}

fn data(value: Option<SeqV>) -> Option<Vec<u8>> {
    value.map(|v| v.data)
}

fn listed(items: Option<Vec<(String, SeqV)>>) -> Option<Vec<(String, Vec<u8>)>> {
    items.map(|items| items.into_iter().map(|(k, v)| (k, v.data)).collect())
}

fn kv(key: &str, value: &str) -> (String, Vec<u8>) {
    (key.to_string(), value.as_bytes().to_vec())
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
    wait_for(&watched, "pp/a", b"a").await?;

    // A ready copy lists every dir under the prefix, the prefix included.
    let all = vec![kv("pp/a", "a"), kv("pp/d/x", "x")];
    assert_eq!(listed(watched.try_list_dir_ready("pp")), Some(all));
    let dir = vec![kv("pp/d/x", "x")];
    assert_eq!(listed(watched.try_list_dir_ready("pp/d/")), Some(dir));
    // Only one trailing `/` is ignored: `pp/d//` lists the keys `pp/d//...`, as meta-service does.
    assert_eq!(listed(watched.try_list_dir_ready("pp/d//")), Some(vec![]));

    // A key the copy lacks, and a dir outside the prefix, `ppp` included, are left to
    // meta-service.
    assert_eq!(data(watched.try_get_ready("pp/missing")), None);
    assert_eq!(data(watched.try_get_ready("ppp/a")), None);
    assert_eq!(listed(watched.try_list_dir_ready("ppp")), None);

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

    assert_eq!(data(watched.try_get_ready("pp/a")), Some(b"a".to_vec()));
    assert_eq!(data(watched.try_get_ready("pp/t")), None);
    let live = vec![kv("pp/a", "a")];
    assert_eq!(listed(watched.try_list_dir_ready("pp")), Some(live));
    Ok(())
}

/// A copy that never becomes ready answers no read, and a read does not wait for it.
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

    assert_eq!(data(watched.try_get_ready("pp/a")), None);
    assert_eq!(listed(watched.try_list_dir_ready("pp")), None);
    Ok(())
}
