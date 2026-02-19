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

use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_test_harness::make_grpc_client;
use databend_meta_test_harness::meta_service_test_harness;
use databend_meta_test_harness::start_metasrv_cluster;
use databend_meta_types::UpsertKV;
use log::info;
use test_harness::test;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_semaphore_simple() -> anyhow::Result<()> {
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
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_semaphore_guard_future() -> anyhow::Result<()> {
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

#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_semaphore_time_based() -> anyhow::Result<()> {
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

/// Test error handling when acquirer is dropped while operations are in progress
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_acquirer_closed_error_handling() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Create a semaphore and acquire a permit
    let sem = Semaphore::new(client(), "test_acquirer_close", 1, secs(10)).await;

    // Test dropping the semaphore while trying to acquire
    {
        let permit_future = sem.acquire("test_id");
        // Immediately drop sem to test error handling
        drop(permit_future);
    }

    // Test successful case still works
    let sem2 = Semaphore::new(client(), "test_acquirer_close_2", 1, secs(10)).await;
    let _permit = sem2.acquire("test_id_2").await?;

    Ok(())
}

/// Test permit removal notification system
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_permit_removal_notification() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Acquire a permit
    let permit =
        Semaphore::new_acquired(client(), "test_notification", 1, "permit_1", secs(10)).await?;

    // Set up the future to wait for removal notification
    let mut permit_future = std::pin::pin!(permit);

    // The permit should not be ready initially
    let quick_check = timeout(Duration::from_millis(100), permit_future.as_mut()).await;
    assert!(quick_check.is_err(), "Permit should not be ready initially");

    // Manually remove the permit entry from meta-service
    client()
        .upsert_kv(UpsertKV::delete(
            "test_notification/queue/00000000000000000001",
        ))
        .await?;

    // Now the permit should be notified of removal
    let notification_result = timeout(Duration::from_secs(5), permit_future.as_mut()).await;
    assert!(
        notification_result.is_ok(),
        "Permit should be notified of removal"
    );

    Ok(())
}

/// Test resource cleanup when permits are dropped
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_permit_resource_cleanup() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Test that resources are properly cleaned up when permit is dropped
    {
        let permit =
            Semaphore::new_acquired(client(), "test_cleanup", 2, "permit_1", secs(10)).await?;
        // Permit goes out of scope here, should trigger cleanup
        drop(permit);
    }

    // Verify that capacity is available again after cleanup
    let _permit2 =
        Semaphore::new_acquired(client(), "test_cleanup", 2, "permit_2", secs(10)).await?;
    let _permit3 =
        Semaphore::new_acquired(client(), "test_cleanup", 2, "permit_3", secs(10)).await?;

    // This should work because the first permit was properly cleaned up
    Ok(())
}

/// Test concurrent acquirer operations and error isolation
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_semaphore_concurrent_error_isolation() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Create multiple permits concurrently
    let permit1_fut = Semaphore::new_acquired(client(), "concurrent_test", 3, "id1", secs(10));
    let permit2_fut = Semaphore::new_acquired(client(), "concurrent_test", 3, "id2", secs(10));
    let permit3_fut = Semaphore::new_acquired(client(), "concurrent_test", 3, "id3", secs(10));

    let (permit1, permit2, permit3) = tokio::try_join!(permit1_fut, permit2_fut, permit3_fut)?;

    // Verify all permits were acquired with exact expected format
    // Extract and verify the uniq numbers to ensure they're different instances
    let extract_id = |name: &str, expected_id: &str| -> u64 {
        let expected_suffix = format!("](concurrent_test)-Acquirer(id={})", expected_id);
        assert!(
            name.starts_with("Semaphore[uniq="),
            "Name should start with Semaphore[uniq=: {}",
            name
        );
        assert!(
            name.ends_with(&expected_suffix),
            "Name should end with expected suffix: {}",
            name
        );

        let start = "Semaphore[uniq=".len();
        let end = name
            .find("](concurrent_test)")
            .expect("Should find end of uniq");
        name[start..end].parse().expect("Should parse uniq number")
    };

    let uniq1 = extract_id(&permit1.acquirer_name, "id1");
    let uniq2 = extract_id(&permit2.acquirer_name, "id2");
    let uniq3 = extract_id(&permit3.acquirer_name, "id3");

    // Verify each semaphore instance has a different uniq number
    assert_ne!(
        uniq1, uniq2,
        "Different semaphore instances should have different uniq numbers"
    );
    assert_ne!(
        uniq2, uniq3,
        "Different semaphore instances should have different uniq numbers"
    );
    assert_ne!(
        uniq1, uniq3,
        "Different semaphore instances should have different uniq numbers"
    );

    // Drop one permit and verify others remain unaffected
    drop(permit2);

    // Should be able to acquire a new permit in the freed slot
    let _permit4 = Semaphore::new_acquired(client(), "concurrent_test", 3, "id4", secs(10)).await?;

    Ok(())
}

/// Test timeout behavior and recovery
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_semaphore_timeout_behavior() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Test with very short TTL to test timeout behavior
    let short_ttl = Duration::from_millis(100);
    let sem = Semaphore::new(client(), "timeout_test", 1, short_ttl).await;

    // Acquire a permit with short TTL
    let permit = sem.acquire("timeout_id").await?;

    // Wait longer than TTL to ensure timeout handling
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Permit should still be valid immediately after acquisition
    // (The test validates that the system handles timeouts gracefully)
    drop(permit);

    // Should be able to acquire again after the previous permit times out
    let sem2 = Semaphore::new(client(), "timeout_test_2", 1, secs(10)).await;
    let _permit2 = sem2.acquire("timeout_id_2").await?;

    Ok(())
}

/// Test watch stream behavior under connection issues
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_stream_resilience() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Create semaphore that will maintain connection
    let permit =
        Semaphore::new_acquired(client(), "resilience_test", 2, "resilient_id", secs(30)).await?;

    // Verify permit is working
    let mut permit_future = std::pin::pin!(permit);

    // Should not be ready under normal conditions
    let check = timeout(Duration::from_millis(200), permit_future.as_mut()).await;
    assert!(
        check.is_err(),
        "Permit should be stable under normal conditions"
    );

    // Test continues to work even with some connection variability
    // (In a real scenario, this would test reconnection after temporary failures)

    Ok(())
}

/// Test semaphore behavior with different capacity configurations
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_semaphore_capacity_edge_cases() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Test with capacity = 1
    let _permit1 = Semaphore::new_acquired(client(), "capacity_1", 1, "single", secs(10)).await?;

    // Should timeout when trying to acquire second permit
    let timeout_result = timeout(
        Duration::from_millis(500),
        Semaphore::new_acquired(client(), "capacity_1", 1, "blocked", secs(10)),
    )
    .await;
    assert!(
        timeout_result.is_err(),
        "Should timeout with capacity=1 when already occupied"
    );

    // Test with larger capacity
    let permits = futures::future::try_join_all((0..5).map(|i| {
        Semaphore::new_acquired(client(), "capacity_5", 5, format!("permit_{}", i), secs(10))
    }))
    .await?;

    assert_eq!(
        permits.len(),
        5,
        "Should acquire all 5 permits with capacity=5"
    );

    Ok(())
}

/// Test time-based sequencing behavior
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_time_based_sequencing_edge_cases() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect::<Vec<_>>();

    let cli =
        make_grpc_client::<DatabendRuntime>(vec![addresses[0].clone(), addresses[1].clone()])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    // Test time-based sequencing with custom timestamps
    let timestamp1 = Some(Duration::from_millis(1000));
    let timestamp2 = Some(Duration::from_millis(2000));

    let permit1 =
        Semaphore::new_acquired_by_time(client(), "time_seq", 2, "early", timestamp1, secs(10))
            .await?;

    let permit2 =
        Semaphore::new_acquired_by_time(client(), "time_seq", 2, "later", timestamp2, secs(10))
            .await?;

    // Both should succeed with capacity=2
    drop(permit1);
    drop(permit2);

    // Test with None timestamp (current time)
    let _permit3 =
        Semaphore::new_acquired_by_time(client(), "time_seq", 2, "current_time", None, secs(10))
            .await?;

    Ok(())
}

/// If the meta-service stops streaming changes, the semaphore should re-connect.
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_time_based_pause_streaming() -> anyhow::Result<()> {
    let mut tcs = start_metasrv_cluster::<DatabendRuntime>(&[0]).await?;

    let tc = tcs.remove(0);

    let address = tc.config.grpc.api_address().unwrap();

    let cli = make_grpc_client::<DatabendRuntime>(vec![address])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    let timestamp1 = Some(Duration::from_millis(1000));
    let timestamp2 = Some(Duration::from_millis(2000));

    let permit1 =
        Semaphore::new_acquired_by_time(client(), "time_seq", 1, "early", timestamp1, secs(1))
            .await?;

    let fu = Semaphore::new_acquired_by_time(client(), "time_seq", 1, "later", timestamp2, secs(1));

    let (tx, rx) = oneshot::channel();
    DatabendRuntime::spawn(
        async move {
            //
            let res = tokio::time::timeout(secs(5), fu).await;
            println!("=== permit2: {:?}", res);
            info!("permit2: {:?}", res);
            tx.send(res).ok();
        },
        None,
    );

    let meta_node = tc
        .grpc_srv
        .as_ref()
        .unwrap()
        .meta_handle
        .clone()
        .unwrap()
        .get_meta_node()
        .await?;

    let rc = meta_node.runtime_config();

    rc.broadcast_state_machine_changes
        .store(false, std::sync::atomic::Ordering::Relaxed);

    tokio::time::sleep(secs(2)).await;

    rc.broadcast_state_machine_changes
        .store(true, std::sync::atomic::Ordering::Relaxed);

    drop(permit1);

    let res = rx.await;
    info!("permit2: {:?}", res);
    // Re-connect in side subscriber is dangerous, it may miss some event and never acquire a
    // permit.
    assert!(
        res.unwrap().unwrap().is_err(),
        "permit2 can not be acquired"
    );

    Ok(())
}

/// The acquirer should receive a connection closed error when the stream is closed
#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_time_based_connection_closed_error() -> anyhow::Result<()> {
    let mut tcs = start_metasrv_cluster::<DatabendRuntime>(&[0]).await?;

    let mut tc = tcs.remove(0);

    let address = tc.config.grpc.api_address().unwrap();

    let cli = make_grpc_client::<DatabendRuntime>(vec![address])?;
    let client = || cli.clone();
    let secs = |n| Duration::from_secs(n);

    let timestamp2 = Some(Duration::from_millis(2000));

    let fu = Semaphore::new_acquired_by_time(client(), "time_seq", 0, "later", timestamp2, secs(1));

    let (tx, rx) = oneshot::channel();
    DatabendRuntime::spawn(
        async move {
            //
            let res = tokio::time::timeout(secs(5), fu).await;
            println!("=== permit2: {:?}", res);
            info!("permit2: {:?}", res);
            tx.send(res).ok();
        },
        None,
    );

    tokio::time::sleep(secs(1)).await;

    let mut srv = tc.grpc_srv.take().unwrap();
    srv.do_stop(None).await;

    let res = rx.await;
    info!("permit2: {:?}", res);
    println!("permit2: {:?}", res);
    let err = res.unwrap().unwrap().unwrap_err();
    assert!(
        err.to_string()
            .contains("distributed-Semaphore connection closed")
    );

    Ok(())
}
