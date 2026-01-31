// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_runtime_api::RuntimeApi;
use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::raft_types::new_log_id;
use databend_common_version::DATABEND_SEMVER;
use databend_meta::meta_node::meta_worker::MetaWorker;
use databend_meta::meta_service::MetaNode;
use log::info;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv_cluster;

/// Test MetaHandle::handle_get_nodes()
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_cluster_nodes() -> anyhow::Result<()> {
    let tc0 = MetaSrvTestContext::<TokioRuntime>::new(0);
    let mut tc1 = MetaSrvTestContext::<TokioRuntime>::new(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![
        tc0.config
            .raft_config
            .raft_api_addr::<TokioRuntime>()
            .await?
            .to_string(),
    ];

    let _mn0 = MetaNode::<TokioRuntime>::start(&tc0.config).await?;

    let runtime1 = TokioRuntime::new_testing("meta-io-rt-ut");
    let meta_handle_1 =
        MetaWorker::create_meta_worker(tc1.config.clone(), Arc::new(runtime1)).await?;
    let meta_handle_1 = Arc::new(meta_handle_1);

    let c = tc1.config.clone();
    let res = meta_handle_1
        .request(move |mn| {
            let fu = async move { mn.join_cluster(&c).await };
            Box::pin(fu)
        })
        .await??;

    assert!(res.is_ok());

    let nodes = meta_handle_1.handle_get_nodes().await?;
    assert_eq!(nodes.len(), 2);

    Ok(())
}

/// Test MetaHandle::handle_get_status()
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_cluster_state() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<TokioRuntime>(&[0, 1]).await?;

    let meta_handle_0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_handle();

    info!("--- write sample data to the cluster ---");
    {
        meta_handle_0
            .request(|mn| {
                Box::pin(async move {
                    mn.write(LogEntry::new(Cmd::UpsertKV(
                        UpsertKV::update("foo", b"foo").with_ttl(Duration::from_secs(3600)),
                    )))
                    .await
                })
            })
            .await??;
        meta_handle_0
            .request(|mn| {
                Box::pin(async move {
                    mn.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
                        "foo2", b"foo2",
                    ))))
                    .await
                })
            })
            .await??;
        meta_handle_0
            .request(|mn| {
                Box::pin(async move {
                    mn.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
                        "foo3", b"foo3",
                    ))))
                    .await
                })
            })
            .await??;
        meta_handle_0
            .request(|mn| {
                Box::pin(async move {
                    mn.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
                        "foo4", b"foo4",
                    ))))
                    .await
                })
            })
            .await??;
        meta_handle_0
            .request(|mn| {
                Box::pin(async move {
                    mn.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
                        "foo5", b"foo5",
                    ))))
                    .await
                })
            })
            .await??;
    }

    info!("--- trigger snapshot ---");
    {
        meta_handle_0
            .request(|mn| {
                Box::pin(async move {
                    mn.raft.trigger().snapshot().await?;
                    mn.raft
                        .wait(Some(Duration::from_secs(1)))
                        .snapshot(new_log_id(1, 0, 11), "trigger build snapshot")
                        .await?;
                    Ok::<_, anyhow::Error>(())
                })
            })
            .await??;
    }

    let version = DATABEND_SEMVER.clone().to_string();
    let status = meta_handle_0.handle_get_status(&version).await?;

    println!(
        "status = {}",
        serde_json::to_string_pretty(&status).unwrap()
    );

    // Assert key fields in the status response
    assert_eq!(status.id, 0);
    assert_eq!(status.state, "Leader");
    assert!(status.is_leader);
    assert_eq!(status.current_term, 1);
    assert_eq!(status.last_log_index, 11);
    assert_eq!(status.snapshot_key_count, 6);
    assert_eq!(
        status.snapshot_key_space_stat,
        BTreeMap::from_iter([("exp-".to_string(), 1), ("kv--".to_string(), 5),])
    );
    assert_eq!(status.last_seq, 5);

    // Verify last_applied structure
    assert_eq!(status.last_applied, new_log_id(1, 0, 11));

    // Verify leader information
    assert!(status.leader.is_some());
    let leader = status.leader.unwrap();
    assert_eq!(leader.name, "0");
    assert!(leader.endpoint.to_string().starts_with("localhost:"));

    // Verify voters and non_voters
    assert_eq!(status.voters.len(), 2);
    assert_eq!(status.non_voters.len(), 0);

    // Verify replication status - check that we have replicas for both nodes
    let replication = status.replication.clone().unwrap();
    assert!(replication.contains_key(&0));
    assert!(replication.contains_key(&1));

    // Verify raft_log fields
    assert!(status.raft_log.cache_items > 0);
    assert!(status.raft_log.cache_used_size > 0);
    assert!(status.raft_log.wal_total_size > 0);
    assert_eq!(status.raft_log.wal_closed_chunk_count, 0);
    assert_eq!(status.raft_log.wal_closed_chunk_total_size, 0);

    Ok(())
}

/// Test MetaHandle::handle_raft_metrics()
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_handle_raft_metrics() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let meta0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_handle();
    let metrics = meta0.handle_raft_metrics().await?.borrow_watched().clone();

    assert_eq!(metrics.current_leader, Some(0));
    assert_eq!(
        metrics.membership_config.membership().voter_ids().count(),
        3
    );

    Ok(())
}

/// Test MetaHandle::handle_trigger_snapshot()
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_handle_trigger_snapshot() -> anyhow::Result<()> {
    let tc = MetaSrvTestContext::<TokioRuntime>::new(0);

    let runtime = TokioRuntime::new_testing("meta-io-rt-ut");
    let meta_handle = MetaWorker::create_meta_worker(tc.config.clone(), Arc::new(runtime)).await?;

    // Trigger snapshot
    let result = meta_handle.handle_trigger_snapshot().await?;
    assert!(result.is_ok());

    Ok(())
}

/// Test MetaHandle::handle_trigger_transfer_leader()
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_handle_trigger_transfer_leader() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let meta0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_handle();
    let metrics = meta0.handle_raft_metrics().await?.borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(0));

    // Transfer leadership to node 2
    let result = meta0.handle_trigger_transfer_leader(2).await?;
    assert!(result.is_ok());

    // Wait for leadership transfer
    tokio::time::sleep(Duration::from_millis(500)).await;

    let metrics = meta0.handle_raft_metrics().await?.borrow_watched().clone();
    assert_eq!(metrics.current_leader, Some(2));

    Ok(())
}

/// Test MetaHandle::handle_get_sys_data()
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_handle_get_sys_data() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;

    let meta0 = tcs[0].grpc_srv.as_ref().unwrap().get_meta_handle();

    let sys_data = meta0.handle_get_sys_data().await?;

    // Verify sys_data contains expected membership info
    let membership = sys_data.last_membership_ref();
    assert_eq!(membership.membership().voter_ids().count(), 3);

    Ok(())
}
