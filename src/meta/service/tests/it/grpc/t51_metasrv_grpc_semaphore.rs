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

use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_semaphore::Semaphore;
use databend_common_meta_types::UpsertKV;
use test_harness::test;
use tokio::time::timeout;

use crate::testing::meta_service_test_harness;
use crate::tests::service::make_grpc_client;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_semaphore_simple() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    let cli = make_grpc_client(vec![a1(), a2(), a0()])?;

    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Two semaphores in this test.
    let s1 = "s1";
    let s2 = "s2";

    // Two semaphore in s1 can be acquired
    let _s1g1 = Semaphore::new_acquired(client(), s1, 2, "id11", secs(3)).await?;
    let s1g2 = Semaphore::new_acquired(client(), s1, 2, "id12", secs(3)).await?;

    // Two semaphore in s2 can be acquired
    let _s2g1 = Semaphore::new_acquired(client(), s2, 2, "id21", secs(3)).await?;
    let _s2g2 = Semaphore::new_acquired(client(), s2, 2, "id22", secs(3)).await?;

    // Another semaphore in s1 can not be acquired

    let fu = Semaphore::new_acquired(client(), s1, 2, "id13", secs(3));
    let s1g3 = timeout(Duration::from_secs(1), fu).await;

    assert!(
        s1g3.is_err(),
        "s1g3 can not acquire because run out of capacity"
    );

    // Release s1g2, then s1g4 can be acquired

    drop(s1g2);

    let _s1g4 = Semaphore::new_acquired(client(), s1, 2, "id14", secs(3)).await?;

    Ok(())
}

/// Assert the acquired guard gets ready when the semaphore is removed.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_semaphore_guard_future() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    let cli = make_grpc_client(vec![a1(), a2(), a0()])?;

    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    let s1g1 = Semaphore::new_acquired(client(), "s1", 2, "id11", secs(3)).await?;

    let mut c = std::pin::pin!(s1g1);

    // The guard does not get ready while the lease is extended.

    let res = timeout(Duration::from_millis(4_000), c.as_mut()).await;
    assert!(res.is_err(), "not ready since lease is extended.");

    // Remove the semaphore entry, the future is notified.

    client()
        .upsert_kv(UpsertKV::delete("s1/queue/00000000000000000001"))
        .await?;

    let res = timeout(Duration::from_millis(100), c.as_mut()).await;
    assert!(res.is_ok(), "acquired guard become ready");

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_semaphore_time_based() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    let cli = make_grpc_client(vec![a1(), a2(), a0()])?;

    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Two semaphores in this test.
    let s1 = "s1";
    let s2 = "s2";

    // Two semaphore in s1 can be acquired
    let _s1g1 = Semaphore::new_acquired_by_time(client(), s1, 2, "id11", None, secs(3)).await?;
    let s1g2 = Semaphore::new_acquired_by_time(client(), s1, 2, "id12", None, secs(3)).await?;

    // Two semaphore in s2 can be acquired
    let _s2g1 = Semaphore::new_acquired_by_time(client(), s2, 2, "id21", None, secs(3)).await?;
    let _s2g2 = Semaphore::new_acquired_by_time(client(), s2, 2, "id22", None, secs(3)).await?;

    // Another semaphore in s1 can not be acquired

    let fu = Semaphore::new_acquired_by_time(client(), s1, 2, "id13", None, secs(3));
    let s1g3 = timeout(Duration::from_secs(1), fu).await;

    assert!(
        s1g3.is_err(),
        "s1g3 can not acquire because run out of capacity"
    );

    // Release s1g2, then s1g4 can be acquired

    drop(s1g2);

    let _s1g4 = Semaphore::new_acquired_by_time(client(), s1, 2, "id14", None, secs(3)).await?;

    Ok(())
}
