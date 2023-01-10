// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_meta_api::KVApi;
use common_meta_sled_store::openraft::LogIdOptionExt;
use common_meta_sled_store::openraft::State;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::Cmd;
use common_meta_types::Endpoint;
use common_meta_types::LogEntry;
use common_meta_types::NodeId;
use common_meta_types::UpsertKV;
use databend_meta::configs;
use databend_meta::init_meta_ut;
use databend_meta::meta_service::ForwardRequest;
use databend_meta::meta_service::ForwardRequestBody;
use databend_meta::meta_service::JoinRequest;
use databend_meta::meta_service::LeaveRequest;
use databend_meta::meta_service::MetaNode;
use maplit::btreeset;
use pretty_assertions::assert_eq;
use tracing::info;

use crate::tests::meta_node::start_meta_node_cluster;
use crate::tests::meta_node::start_meta_node_leader;
use crate::tests::meta_node::start_meta_node_non_voter;
use crate::tests::meta_node::timeout;
use crate::tests::service::MetaSrvTestContext;

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_boot() -> anyhow::Result<()> {
    // - Start a single node meta service cluster.
    // - Test the single node is recorded by this cluster.

    let tc = MetaSrvTestContext::new(0);
    let addr = tc.config.raft_config.raft_api_advertise_host_endpoint();

    let mn = MetaNode::boot(&tc.config).await?;

    let got = mn.get_node(&0).await?;
    assert_eq!(addr, got.unwrap().endpoint);
    mn.stop().await?;
    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_graceful_shutdown() -> anyhow::Result<()> {
    // - Start a leader then shutdown.

    let (_nid0, tc) = start_meta_node_leader().await?;
    let mn0 = tc.meta_node();

    let mut rx0 = mn0.raft.metrics();

    let joined = mn0.stop().await?;
    assert_eq!(3, joined);

    // tx closed:
    loop {
        let r = rx0.changed().await;
        if r.is_err() {
            info!("done!!!");
            break;
        }

        info!("st: {:?}", rx0.borrow());
    }
    assert!(rx0.changed().await.is_err());
    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_join() -> anyhow::Result<()> {
    // - Bring up a cluster
    // - Join a new node by sending a Join request to leader.
    // - Join a new node by sending a Join request to a non-voter.
    // - Restart all nodes and check if states are restored.

    let span = tracing::span!(tracing::Level::INFO, "test_meta_node_join");
    let _ent = span.enter();

    let (mut _nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![1]).await?;
    let mut all = test_context_nodes(&tcs);
    let tc0 = tcs.remove(0);
    let tc1 = tcs.remove(0);

    info!("--- bring up non-voter 2");

    let node_id = 2;
    let tc2 = MetaSrvTestContext::new(node_id);

    let mn2 = MetaNode::open_create(&tc2.config.raft_config, None, Some(())).await?;

    info!("--- join non-voter 2 to cluster by leader");

    let leader_id = all[0].get_leader().await?.unwrap();
    let leader = all[leader_id as usize].clone();

    let admin_req = join_req(
        node_id,
        tc2.config.raft_config.raft_api_addr().await?,
        tc2.config.grpc_api_address.clone(),
        0,
    );
    leader.handle_forwardable_request(admin_req).await?;

    all.push(mn2.clone());

    info!("--- check all nodes has node-3 joined");
    {
        for mn in all.iter() {
            mn.raft
                .wait(timeout())
                .members(btreeset! {0,2}, format!("node-2 is joined: {}", mn.sto.id))
                .await?;
        }
    }

    info!("--- bring up non-voter 3");

    let node_id = 3;
    let tc3 = MetaSrvTestContext::new(node_id);
    let mn3 = MetaNode::open_create(&tc3.config.raft_config, None, Some(())).await?;

    info!("--- join node-3 by sending rpc `join` to a non-leader");
    {
        let to_addr = tc1.config.raft_config.raft_api_addr().await?;

        let mut client = RaftServiceClient::connect(format!("http://{}", to_addr)).await?;
        let admin_req = join_req(
            node_id,
            tc3.config.raft_config.raft_api_addr().await?,
            tc3.config.grpc_api_address.clone(),
            1,
        );
        client.forward(admin_req).await?;
    }

    info!("--- check all nodes has node-3 joined");

    all.push(mn3.clone());
    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(
                btreeset! {0,2,3},
                format!("node-3 is joined: {}", mn.sto.id),
            )
            .await?;
    }

    info!("--- stop all meta node");

    for mn in all.drain(..) {
        mn.stop().await?;
    }

    info!("--- re-open all meta node");

    let mn0 = MetaNode::open_create(&tc0.config.raft_config, Some(()), None).await?;
    let mn1 = MetaNode::open_create(&tc1.config.raft_config, Some(()), None).await?;
    let mn2 = MetaNode::open_create(&tc2.config.raft_config, Some(()), None).await?;
    let mn3 = MetaNode::open_create(&tc3.config.raft_config, Some(()), None).await?;

    let all = vec![mn0, mn1, mn2, mn3];

    info!("--- check reopened memberships");

    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(btreeset! {0,2,3}, format!("node-{} membership", mn.sto.id))
            .await?;
    }

    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_leave() -> anyhow::Result<()> {
    // - Bring up a cluster
    // - Leave a     voter node by sending a Leave request to a non-voter.
    // - Leave a non-voter node by sending a Leave request to a non-voter.
    // - Restart all nodes and check if states are restored.

    let (mut log_index, tcs) = start_meta_node_cluster(btreeset![0, 1, 2], btreeset![3]).await?;
    let mut all = test_context_nodes(&tcs);

    let leader_id = 0;
    let leave_node_id = 1;

    let leader = all[leader_id as usize].clone();

    info!("--- leave voter node-1");
    {
        let req = ForwardRequest {
            forward_to_leader: 0,
            body: ForwardRequestBody::Leave(LeaveRequest {
                node_id: leave_node_id,
            }),
        };

        leader.handle_forwardable_request(req).await?;
        // Change membership
        log_index += 2;
        // Remove node
        log_index += 1;

        leader
            .raft
            .wait(timeout())
            .log(Some(log_index), "commit leave-request logs for node-1")
            .await?;

        leader
            .raft
            .wait(timeout())
            .members(btreeset! {0,2}, "node-1 left the cluster")
            .await?;
    }

    info!("--- check nodes list: node-1 is removed");
    {
        let nodes = leader.get_nodes().await?;
        assert_eq!(
            vec!["0", "2", "3"],
            nodes.iter().map(|x| x.name.clone()).collect::<Vec<_>>()
        );
    }

    info!("--- leave non-voter node-3");
    {
        let req = ForwardRequest {
            forward_to_leader: 0,
            body: ForwardRequestBody::Leave(LeaveRequest { node_id: 3 }),
        };

        leader.handle_forwardable_request(req).await?;
        // Remove node, no membership change
        log_index += 1;

        leader
            .raft
            .wait(timeout())
            .log(Some(log_index), "commit leave-request logs for node-3")
            .await?;
    }

    info!("--- check nodes list: node-3 is removed");
    {
        let nodes = leader.get_nodes().await?;
        assert_eq!(
            vec!["0", "2"],
            nodes.iter().map(|x| x.name.clone()).collect::<Vec<_>>()
        );
    }

    info!("--- stop all meta node");
    {
        for mn in all.drain(..) {
            mn.stop().await?;
        }
    }

    // restart the cluster and check membership
    info!("--- re-open all meta node");

    let tc0 = &tcs[0];
    let tc2 = &tcs[2];

    let mn0 = MetaNode::open_create(&tc0.config.raft_config, Some(()), None).await?;
    let mn2 = MetaNode::open_create(&tc2.config.raft_config, Some(()), None).await?;

    let all = vec![mn0, mn2];

    info!("--- check reopened memberships");

    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(btreeset! {0,2}, format!("node-{} membership", mn.sto.id))
            .await?;
    }

    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_join_rejoin() -> anyhow::Result<()> {
    // - Bring up a cluster
    // - Join a new node.
    // - Join another new node twice.

    let (mut _nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let mut all = test_context_nodes(&tcs);
    let _tc0 = tcs.remove(0);

    info!("--- bring up non-voter 1");

    let node_id = 1;
    let tc1 = MetaSrvTestContext::new(node_id);

    let mn1 = MetaNode::open_create(&tc1.config.raft_config, None, Some(())).await?;

    info!("--- join non-voter 1 to cluster");

    let leader_id = all[0].get_leader().await?.unwrap();
    let leader = all[leader_id as usize].clone();
    let req = join_req(
        node_id,
        tc1.config.raft_config.raft_api_addr().await?,
        tc1.config.grpc_api_address,
        1,
    );
    leader.handle_forwardable_request(req).await?;

    all.push(mn1.clone());

    info!("--- check all nodes has node-1 joined");
    {
        for mn in all.iter() {
            mn.raft
                .wait(timeout())
                .members(btreeset! {0,1}, format!("node-1 is joined: {}", mn.sto.id))
                .await?;
        }
    }

    info!("--- bring up non-voter 3");

    let node_id = 2;
    let tc2 = MetaSrvTestContext::new(node_id);

    let mn2 = MetaNode::open_create(&tc2.config.raft_config, None, Some(())).await?;

    info!("--- join node-2 by sending rpc `join` to a non-leader");
    {
        let req = join_req(
            node_id,
            tc2.config.raft_config.raft_api_addr().await?,
            tc2.config.grpc_api_address.clone(),
            1,
        );
        leader.handle_forwardable_request(req).await?;
    }
    info!("--- join node-2 again");
    {
        let req = join_req(
            node_id,
            tc2.config.raft_config.raft_api_addr().await?,
            tc2.config.grpc_api_address,
            1,
        );
        mn1.handle_forwardable_request(req).await?;
    }

    all.push(mn2.clone());

    info!("--- check all nodes has node-3 joined");

    for mn in all.iter() {
        mn.raft
            .wait(timeout())
            .members(
                btreeset! {0,1,2},
                format!("node-2 is joined: {}", mn.sto.id),
            )
            .await?;
    }

    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_join_with_state() -> anyhow::Result<()> {
    // Assert that MetaNode allows joining even with initialized store.
    // But does not allow joining with a store that already has membership initialized.
    //
    // In this test it needs a cluster of 3 to form a quorum of 2, so that node-2 can be stopped.

    let tc0 = MetaSrvTestContext::new(0);

    let mut tc1 = MetaSrvTestContext::new(1);
    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    let mut tc2 = MetaSrvTestContext::new(2);
    tc2.config.raft_config.single = false;
    tc2.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr().await?.to_string()];

    let meta_node = MetaNode::start(&tc0.config).await?;
    // Initial log and add node-0 log.
    let mut log_index = 2;

    let res = meta_node
        .join_cluster(&tc0.config.raft_config, tc0.config.grpc_api_address)
        .await?;
    assert_eq!(Err("Did not join: --join is empty".to_string()), res);

    let meta_node1 = MetaNode::start(&tc1.config).await?;
    let res = meta_node1
        .join_cluster(&tc1.config.raft_config, tc1.config.grpc_api_address.clone())
        .await?;
    assert_eq!(Ok(()), res);

    // Two membership logs, one add-node log;
    log_index += 3;
    meta_node1
        .raft
        .wait(timeout())
        .log(Some(log_index), "node-1 join cluster")
        .await?;

    info!("--- initialize store for node-2");
    {
        let n2 = MetaNode::start(&tc2.config).await?;
        n2.stop().await?;
    }

    info!("--- Allow to join node-2 with initialized store");
    {
        let n2 = MetaNode::start(&tc2.config).await?;
        let res = n2
            .join_cluster(&tc2.config.raft_config, tc2.config.grpc_api_address.clone())
            .await?;
        assert_eq!(Ok(()), res);

        // Two membership logs, one add-node log;
        log_index += 3;

        // Add this barrier to ensure all of the logs are applied before quit.
        // Otherwise the next time node-2 starts it can not see the applied
        // membership and believes it has not yet joined into a cluster.
        n2.raft
            .wait(timeout())
            .log(Some(log_index), "node-2 join cluster")
            .await?;

        n2.stop().await?;
    }

    info!("--- Not allowed to join node-2 with store with membership");
    {
        let n2 = MetaNode::start(&tc2.config).await?;
        let res = n2
            .join_cluster(&tc2.config.raft_config, tc2.config.grpc_api_address)
            .await?;
        assert_eq!(
            Err("Did not join: node 2 already in cluster".to_string()),
            res
        );

        n2.stop().await?;
    }
    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_restart() -> anyhow::Result<()> {
    // TODO check restarted follower.
    // - Start a leader and a non-voter;
    // - Restart them.
    // - Check old data an new written data.

    let (_nid0, tc0) = start_meta_node_leader().await?;
    let mn0 = tc0.meta_node();

    let (_nid1, tc1) = start_meta_node_non_voter(mn0.clone(), 1).await?;
    let mn1 = tc1.meta_node();

    let sto0 = mn0.sto.clone();
    let sto1 = mn1.sto.clone();

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    assert_upsert_kv_synced(meta_nodes.clone(), "key1").await?;

    // stop
    info!("shutting down all");

    let n = mn0.stop().await?;
    assert_eq!(3, n);
    let n = mn1.stop().await?;
    assert_eq!(3, n);

    info!("restart all");

    // restart
    let config = configs::Config::default();
    let mn0 = MetaNode::builder(&config.raft_config)
        .node_id(0)
        .sto(sto0)
        .build()
        .await?;
    let mn1 = MetaNode::builder(&config.raft_config)
        .node_id(1)
        .sto(sto1)
        .build()
        .await?;

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    mn0.raft
        .wait(timeout())
        .state(State::Leader, "leader restart")
        .await?;
    mn1.raft
        .wait(timeout())
        .state(State::Learner, "learner restart")
        .await?;
    mn0.raft.add_learner(1, false).await?;
    mn1.raft
        .wait(timeout())
        .current_leader(0, "node-1 has leader")
        .await?;

    assert_upsert_kv_synced(meta_nodes.clone(), "key2").await?;

    // check old data
    assert_get_kv(meta_nodes, "key1", "key1").await?;

    Ok(())
}

#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_restart_single_node() -> anyhow::Result<()> {
    // TODO(xp): This function will replace `test_meta_node_restart` after fs backed state machine is ready.

    // Test fs backed meta node restart.
    // - Start a cluster of a solo leader;
    // - Write one log.
    // - Restart.
    // - Check node state:
    //   - raft hard state
    //   - raft logs.
    //   - state machine:
    //     - Nodes
    //   - TODO(xp): snapshot is empty, since snapshot is not persisted in this version see `MetaStore`.
    // - Check cluster:
    //   - Leader is elected.
    //   - TODO(xp): Leader starts replication to follower and non-voter.
    //   - TODO(xp): New log will be successfully written and sync
    //   - TODO(xp): A new snapshot will be created and transferred  on demand.

    let mut log_index: u64 = 0;
    let (_id, tc) = start_meta_node_leader().await?;
    // initial membeship, leader blank, add node
    log_index += 2;

    let want_hs;
    {
        let leader = tc.meta_node();

        leader
            .assume_leader()
            .await?
            .write(LogEntry {
                txid: None,
                time_ms: None,
                cmd: Cmd::UpsertKV(UpsertKV::update("foo", b"1")),
            })
            .await?;
        log_index += 1;

        want_hs = leader.sto.raft_state.read_hard_state()?;

        leader.stop().await?;
    }

    info!("--- reopen MetaNode");

    let raft_conf = &tc.config.raft_config;

    let leader = MetaNode::open_create(raft_conf, Some(()), None).await?;

    log_index += 1;

    leader
        .raft
        .wait(timeout())
        .state(State::Leader, "leader restart")
        .await?;
    leader
        .raft
        .wait(timeout())
        .log(
            Some(log_index),
            format!(
                "reopened: applied index at {} for node-{}",
                log_index, leader.sto.id
            ),
        )
        .await?;

    info!("--- check hard state");
    {
        let hs = leader.sto.raft_state.read_hard_state()?;
        assert_eq!(want_hs, hs);
    }

    info!("--- check logs");
    {
        let logs = leader.sto.log.range_values(..)?;
        info!("logs: {:?}", logs);
        assert_eq!(log_index as usize + 1, logs.len());
    }

    info!("--- check state machine: nodes");
    {
        let node = leader.sto.get_node(&0).await?.unwrap();
        assert_eq!(
            tc.config.raft_config.raft_api_advertise_host_endpoint(),
            node.endpoint
        );
    }

    Ok(())
}

fn join_req(
    node_id: NodeId,
    endpoint: Endpoint,
    grpc_api_addr: String,
    forward: u64,
) -> ForwardRequest {
    ForwardRequest {
        forward_to_leader: forward,
        body: ForwardRequestBody::Join(JoinRequest {
            node_id,
            endpoint,
            grpc_api_addr,
        }),
    }
}

/// Write one log on leader, check all nodes replicated the log.
/// Returns the number log committed.
async fn assert_upsert_kv_synced(meta_nodes: Vec<Arc<MetaNode>>, key: &str) -> anyhow::Result<u64> {
    let leader_id = meta_nodes[0].get_leader().await?.unwrap();
    let leader = meta_nodes[leader_id as usize].clone();

    let last_applied = leader.raft.metrics().borrow().last_applied;
    info!("leader: last_applied={:?}", last_applied);
    {
        leader
            .assume_leader()
            .await?
            .write(LogEntry {
                txid: None,
                time_ms: None,
                cmd: Cmd::UpsertKV(UpsertKV::update(key, key.as_bytes())),
            })
            .await?;
    }

    // Assert applied index on every node
    for (_i, mn) in meta_nodes.iter().enumerate() {
        mn.raft
            .wait(timeout())
            .log(
                Some(last_applied.next_index()),
                format!(
                    "check upsert-kv has applied index at {} for node-{}",
                    last_applied.next_index(),
                    mn.sto.id
                ),
            )
            .await?;
    }
    assert_get_kv(meta_nodes.clone(), key, key).await?;

    Ok(1)
}

async fn assert_get_kv(
    meta_nodes: Vec<Arc<MetaNode>>,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    for (i, mn) in meta_nodes.iter().enumerate() {
        let got = mn.get_kv(key).await?;
        assert_eq!(
            value.to_string().into_bytes(),
            got.unwrap().data,
            "n{} applied value",
            i
        );
    }
    Ok(())
}

fn test_context_nodes(tcs: &[MetaSrvTestContext]) -> Vec<Arc<MetaNode>> {
    tcs.iter().map(|tc| tc.meta_node()).collect::<Vec<_>>()
}
