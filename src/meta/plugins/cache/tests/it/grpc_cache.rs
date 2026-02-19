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

use std::time::Duration;

use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::make_grpc_client;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use databend_meta_types::normalize_meta::NormalizeMeta;
use log::debug;
use test_harness::test;
use tokio::time::sleep;

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

fn s(x: impl ToString) -> String {
    x.to_string()
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().into_bytes()
}
