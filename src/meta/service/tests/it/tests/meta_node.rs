// Copyright 2022 Datafuse Labs.
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

//! Supporting util for MetaNode tests

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_sled_store::openraft::ServerState;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_meta::meta_service::MetaNode;
use databend_meta::Opened;
use log::info;
use maplit::btreeset;

use crate::tests::service::MetaSrvTestContext;

/// Setup a cluster with several voter and several non_voter
/// The node id 0 must be in `voters` and node 0 is elected as leader.
pub(crate) async fn start_meta_node_cluster(
    voters: BTreeSet<NodeId>,
    non_voters: BTreeSet<NodeId>,
) -> anyhow::Result<(u64, Vec<MetaSrvTestContext>)> {
    // leader is always node-0
    assert!(voters.contains(&0));
    assert!(!non_voters.contains(&0));

    let mut test_contexts = vec![];

    let (_id, tc0) = start_meta_node_leader().await?;
    let leader = tc0.meta_node();
    test_contexts.push(tc0);

    // initialization log, blank log, add node
    let mut log_index = 3;
    leader
        .raft
        .wait(timeout())
        .applied_index(Some(log_index), "start leader")
        .await?;

    for id in voters.iter() {
        // leader is already created.
        if *id == 0 {
            continue;
        }
        let (_id, tc) = start_meta_node_non_voter(leader.clone(), *id).await?;

        // Adding a node, change membership
        log_index += 2;
        tc.meta_node()
            .raft
            .wait(timeout())
            .applied_index(Some(log_index), format!("add :{}", id))
            .await?;

        test_contexts.push(tc);
    }

    for id in non_voters.iter() {
        let (_id, tc) = start_meta_node_non_voter(leader.clone(), *id).await?;

        // Adding a node, change membership
        log_index += 2;

        tc.meta_node()
            .raft
            .wait(timeout())
            .applied_index(Some(log_index), format!("add :{}", id))
            .await?;
        // wait_for_log(&tc.meta_nodes[0], log_index).await?;

        test_contexts.push(tc);
    }

    if voters != btreeset! {0} {
        leader.raft.change_membership(voters.clone(), true).await?;
        log_index += 2;
    }

    info!("--- check node roles");
    {
        leader
            .raft
            .wait(timeout())
            .state(ServerState::Leader, "check leader state")
            .await?;

        for item in test_contexts.iter().take(voters.len()).skip(1) {
            item.meta_node()
                .raft
                .wait(timeout())
                .state(
                    ServerState::Follower,
                    format!("check follower-{} state", item.meta_node().sto.id),
                )
                .await?;
        }
        for item in test_contexts
            .iter()
            .skip(voters.len())
            .take(non_voters.len())
        {
            item.meta_node()
                .raft
                .wait(timeout())
                .state(
                    ServerState::Learner,
                    format!("check learner-{} state", item.meta_node().sto.id),
                )
                .await?;
        }
    }

    info!("--- check node logs");
    {
        for tc in &test_contexts {
            tc.meta_node()
                .raft
                .wait(timeout())
                .applied_index(
                    Some(log_index),
                    format!(
                        "check applied index: {} for node-{}",
                        log_index,
                        tc.meta_node().sto.id
                    ),
                )
                .await?;
        }
    }

    Ok((log_index, test_contexts))
}

pub(crate) async fn start_meta_node_leader() -> anyhow::Result<(NodeId, MetaSrvTestContext)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    let nid = 0;
    let mut tc = MetaSrvTestContext::new(nid);
    let addr = tc.config.raft_config.raft_api_advertise_host_endpoint();

    dbg!(&tc.config.raft_config.raft_dir);

    // boot up a single-node cluster
    let mn = MetaNode::boot(&tc.config).await?;

    tc.meta_node = Some(mn.clone());

    {
        tc.assert_raft_server_connection().await?;

        // assert that boot() adds the node to meta.
        let got = mn.get_node(&nid).await;
        assert_eq!(addr, got.unwrap().endpoint, "nid0 is added");

        mn.raft
            .wait(timeout())
            .state(ServerState::Leader, "leader started")
            .await?;

        mn.raft
            .wait(timeout())
            .current_leader(0, "node-0 has leader")
            .await?;
    }
    Ok((nid, tc))
}

/// Start a NonVoter and setup replication from leader to it.
/// Assert the NonVoter is ready and upto date such as the known leader, state and grpc service.
pub(crate) async fn start_meta_node_non_voter(
    leader: Arc<MetaNode>,
    id: NodeId,
) -> anyhow::Result<(NodeId, MetaSrvTestContext)> {
    let mut tc = MetaSrvTestContext::new(id);
    let addr = tc.config.raft_config.raft_api_addr().await?;

    let raft_conf = &tc.config.raft_config;

    let mn = MetaNode::open_create(raft_conf, None, Some(())).await?;

    // // Disable heartbeat, because in openraft v0.8 heartbeat is a blank log.
    // // Log index becomes non-deterministic.
    // mn.raft.enable_heartbeat(false);

    assert!(!mn.is_opened());

    tc.meta_node = Some(mn.clone());

    {
        // add node to cluster as a non-voter
        let resp = leader
            .add_node(
                id,
                Node::new(id, addr.clone())
                    .with_grpc_advertise_address(tc.config.grpc_api_advertise_address()),
            )
            .await?;
        match resp {
            AppliedState::Node { prev: _, result } => {
                assert_eq!(addr.clone(), result.unwrap().endpoint);
            }
            _ => {
                panic!("expect node")
            }
        }
    }

    {
        tc.assert_raft_server_connection().await?;
        mn.raft
            .wait(timeout())
            .current_leader(0, "non-voter has leader")
            .await?;
    }

    Ok((id, tc))
}

/// Make a default timeout for wait() for test.
pub(crate) fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(10_000))
}
