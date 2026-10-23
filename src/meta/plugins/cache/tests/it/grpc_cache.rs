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

//! Test special cases of grpc API: transaction().

use std::sync::Arc;
use std::time::Duration;

use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_client::runtime_api::RuntimeApi;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::normalize_meta::NormalizeMeta;
use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use log::debug;
use test_harness::test;
use tokio::time::sleep;

fn make_grpc_client<R: RuntimeApi>(
    addresses: Vec<String>,
) -> Result<Arc<ClientHandle<R>>, CreationError> {
    MetaGrpcClient::<R>::try_create(
        addresses,
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )
}

#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_cache_basic() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    let cli = make_grpc_client::<DatabendRuntime>(vec![a1(), a2(), a0()])?;

    let client = || cli.clone();

    cli.upsert_kv(UpsertKV::update("po/a", b"x")).await?;

    cli.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;
    cli.upsert_kv(UpsertKV::update("pp/c", b"c")).await?;

    cli.upsert_kv(UpsertKV::update("pq/d", b"d")).await?;

    let mut c = Cache::new(client(), "pp", "test").await;

    // Check initial state

    let got = c.try_list_dir("pp").await?;
    assert_eq!(
        vec![
            (s("pp/a"), SeqV::new(2, b("a"))),
            (s("pp/c"), SeqV::new(3, b("c"))),
        ],
        got.without_proposed_at()
    );

    let last_seq = c.try_last_seq().await?;
    assert_eq!(last_seq, 3);
    assert_eq!(
        c.try_get_ready("pp/c").without_proposed_at(),
        Some(SeqV::new(3, b("c")))
    );
    assert!(c.try_get_ready("pp/missing").is_none());

    // Receive update

    cli.upsert_kv(UpsertKV::update("pp/b", b"b")).await?;
    cli.upsert_kv(UpsertKV::update("pp/c", b"c2")).await?;
    cli.upsert_kv(UpsertKV::delete("pp/a")).await?;

    sleep(Duration::from_millis(500)).await;

    let got = c.try_get("pp/b").await?;
    assert_eq!(got.without_proposed_at(), Some(SeqV::new(5, b("b"))));

    let got = c.try_get("pp/c").await?;
    assert_eq!(got.without_proposed_at(), Some(SeqV::new(6, b("c2"))));

    let got = c.try_get("pp/a").await?;
    assert_eq!(got, None);

    Ok(())
}

/// Test cache survive leader down and switch
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_cache_when_leader_down() -> anyhow::Result<()> {
    let mut tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    debug!("foofoo");

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    // a0() will be shut down
    let cli = make_grpc_client::<DatabendRuntime>(vec![a1(), a2(), a0()])?;

    let client = || cli.clone();

    cli.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;
    cli.upsert_kv(UpsertKV::update("pp/c", b"c")).await?;

    let mut c = Cache::new(client(), "pp", "test").await;

    cli.upsert_kv(UpsertKV::update("pp/after_cache_create", b"x"))
        .await?;

    let got = c.try_get("pp/c").await?;
    assert_eq!(got.without_proposed_at(), Some(SeqV::new(2, b("c"))));

    // Stop the first node, which is the leader
    let mut stopped = tcs.remove(0);
    { stopped }.grpc_srv.take().unwrap().do_stop(None).await;

    sleep(Duration::from_secs(6)).await;

    // Receive update

    cli.upsert_kv(UpsertKV::update("pp/after_stop_n0", b"3"))
        .await?;
    cli.upsert_kv(UpsertKV::update("pp/c", b"c2")).await?;

    sleep(Duration::from_secs(3)).await;

    let got = c.try_get("pp/after_stop_n0").await?;
    assert_eq!(got.without_proposed_at(), Some(SeqV::new(4, b("3"))));

    let got = c.try_get("pp/c").await?;
    assert_eq!(got.without_proposed_at(), Some(SeqV::new(5, b("c2"))));

    Ok(())
}

/// A watch pinned to a dead node must not block callers whose ordinary KV
/// client can still use the remaining nodes.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_ready_cache_read_during_reconnect() -> anyhow::Result<()> {
    let mut tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;
    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();
    let writer = make_grpc_client::<DatabendRuntime>(addresses[1..].to_vec())?;
    writer
        .upsert_kv(UpsertKV::update("ready/a", b"old"))
        .await?;
    let watch_client = make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone()])?;
    let mut cache = Cache::new(watch_client, "ready", "reconnect-ready").await;
    assert!(cache.try_get("ready/a").await?.is_some());

    let mut stopped = tcs.remove(0);
    { stopped }.grpc_srv.take().unwrap().do_stop(None).await;
    sleep(Duration::from_secs(6)).await;

    writer
        .upsert_kv(UpsertKV::update("ready/a", b"new"))
        .await?;
    assert_eq!(writer.get_kv("ready/a").await?.unwrap().data, b("new"));
    // The old blocking try_get would wait for this watch to reconnect forever.
    assert!(cache.try_get_ready("ready/a").is_none());
    Ok(())
}

fn s(x: impl ToString) -> String {
    x.to_string()
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}
