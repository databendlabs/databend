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

//! The watch of a `Cache` is served by the meta-service leader, and follows the leadership.

use std::sync::Arc;
use std::time::Duration;

use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::types::UpsertKV;
use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use test_harness::test;
use tokio::time::sleep;

type TestContext = MetaSrvTestContext<DatabendRuntime>;

fn new_client(addresses: Vec<String>) -> anyhow::Result<Arc<ClientHandle<DatabendRuntime>>> {
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        addresses,
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    Ok(client)
}

/// The number of watch streams each meta node serves.
async fn watchers(tcs: &[TestContext]) -> anyhow::Result<Vec<usize>> {
    let mut counts = Vec::with_capacity(tcs.len());
    for tc in tcs {
        let meta_node = tc.grpc_srv.as_ref().unwrap().get_meta_node().await;
        let count = meta_node
            .dispatcher_handle
            .request_blocking(|d| d.watch_senders().len())
            .await?;
        counts.push(count);
    }
    Ok(counts)
}

/// Wait until the meta node `serving` serves one watch stream, and the others serve none.
async fn wait_for_watch_on(tcs: &[TestContext], serving: usize) -> anyhow::Result<()> {
    let mut want = vec![0; tcs.len()];
    want[serving] = 1;
    for _ in 0..200 {
        if watchers(tcs).await? == want {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    let got = watchers(tcs).await?;
    anyhow::bail!("the meta nodes serve {got:?} watch streams, not {want:?}")
}

/// Move the leadership from `from` to `to`, and wait until `to` leads.
async fn transfer_leader(
    tcs: &[TestContext],
    addresses: &[String],
    from: usize,
    to: usize,
) -> anyhow::Result<()> {
    let meta_node = tcs[from].grpc_srv.as_ref().unwrap().get_meta_node().await;
    meta_node.raft.trigger().transfer_leader(to as u64).await?;

    let to_client = new_client(vec![addresses[to].clone()])?;
    for _ in 0..100 {
        if to_client.get_cluster_status().await?.is_leader {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("meta node {to} did not become the leader")
}

/// A cache opens its watch on the leader, though its client asks a follower first, and moves the
/// watch to the new leader once the leadership moves.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_cache_watches_the_leader() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;
    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    // A new client first connects to the smallest address: make sure that one does not lead.
    let first = (0..addresses.len()).min_by_key(|&i| &addresses[i]).unwrap();
    let mut leader = 0;
    if first == leader {
        transfer_leader(&tcs, &addresses, leader, 1).await?;
        leader = 1;
    }

    let writer = new_client(addresses.clone())?;
    writer.upsert_kv(UpsertKV::update("pp/a", b"a")).await?;

    let cache = Cache::new(new_client(addresses.clone())?, "pp", "test-leader").await;
    // Waits for the initialization.
    let got = cache.try_get("pp/a").await?;
    assert_eq!(got.map(|v| v.data), Some(b"a".to_vec()));
    wait_for_watch_on(&tcs, leader).await?;

    let next = (leader + 1) % tcs.len();
    transfer_leader(&tcs, &addresses, leader, next).await?;
    wait_for_watch_on(&tcs, next).await?;

    writer.upsert_kv(UpsertKV::update("pp/a", b"a2")).await?;
    for _ in 0..100 {
        let got = cache.try_get_ready("pp/a").map(|v| v.data);
        if got.as_deref() == Some(b"a2".as_slice()) {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("the cache did not see the change made after the leadership moved")
}
