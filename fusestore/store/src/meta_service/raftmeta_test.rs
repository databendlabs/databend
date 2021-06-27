// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use async_raft::RaftMetrics;
use async_raft::State;
use common_runtime::tokio;
use common_runtime::tokio::time::Duration;
use common_tracing::tracing;
use maplit::hashset;
use pretty_assertions::assert_eq;

use crate::meta_service::AppliedState;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::MetaNode;
use crate::meta_service::NodeId;
use crate::meta_service::RaftTxId;
use crate::meta_service::RetryableError;
use crate::tests::assert_meta_connection;
use crate::tests::Seq;

// test cases fro Cmd::IncrSeq:
// case_name, txid, key, want
pub fn cases_incr_seq() -> Vec<(&'static str, Option<RaftTxId>, &'static str, u64)> {
    vec![
        ("incr on none", Some(RaftTxId::new("foo", 1)), "k1", 1),
        ("incr on existent", Some(RaftTxId::new("foo", 2)), "k1", 2),
        (
            "dup: same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            2,
        ),
        (
            "diff client, same serial, not a dup request",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            1,
        ),
        ("no txid, no de-dup", None, "k2", 2),
    ]
}

// test cases for Cmd::AddFile
// case_name, txid, key, value, want_prev, want_result
pub fn cases_add_file() -> Vec<(
    &'static str,
    Option<RaftTxId>,
    &'static str,
    &'static str,
    Option<String>,
    Option<String>,
)> {
    vec![
        (
            "add on none",
            Some(RaftTxId::new("foo", 1)),
            "k1",
            "v1",
            None,
            Some("v1".to_string()),
        ),
        (
            "add on existent",
            Some(RaftTxId::new("foo", 2)),
            "k1",
            "v2",
            Some("v1".to_string()),
            None,
        ),
        (
            "dup set with same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            "v3",
            Some("v1".to_string()),
            None,
        ),
        (
            "diff client, same serial",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            "v3",
            None,
            Some("v3".to_string()),
        ),
        ("no txid", None, "k3", "v4", None, Some("v4".to_string())),
    ]
}

// test cases for Cmd::SetFile
// case_name, txid, key, value, want_prev, want_result
pub fn cases_set_file() -> Vec<(
    &'static str,
    Option<RaftTxId>,
    &'static str,
    &'static str,
    Option<String>,
    Option<String>,
)> {
    vec![
        (
            "set on none",
            Some(RaftTxId::new("foo", 1)),
            "k1",
            "v1",
            None,
            Some("v1".to_string()),
        ),
        (
            "set on existent",
            Some(RaftTxId::new("foo", 2)),
            "k1",
            "v2",
            Some("v1".to_string()),
            Some("v2".to_string()),
        ),
        (
            "dup set with same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            "v3",
            Some("v1".to_string()),
            Some("v2".to_string()),
        ),
        (
            "diff client, same serial",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            "v3",
            None,
            Some("v3".to_string()),
        ),
        (
            "no txid",
            None,
            "k2",
            "v4",
            Some("v3".to_string()),
            Some("v4".to_string()),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_boot() -> anyhow::Result<()> {
    // - Start a single node meta service cluster.
    // - Test the single node is recorded by this cluster.

    common_tracing::init_default_tracing();

    let addr = new_addr();
    let resp = MetaNode::boot(0, addr.clone()).await;
    assert!(resp.is_ok());

    let mn = resp.unwrap();

    let got = mn.get_node(&0).await;
    assert_eq!(addr, got.unwrap().address);
    mn.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_graceful_shutdown() -> anyhow::Result<()> {
    // - Start a leader then shutdown.

    common_tracing::init_default_tracing();

    let (_nid0, mn0) = setup_leader().await?;

    let mut rx0 = mn0.raft.metrics();

    let joined = mn0.stop().await?;
    assert_eq!(3, joined);

    // tx closed:
    loop {
        let r = rx0.changed().await;
        if r.is_err() {
            tracing::info!("done!!!");
            break;
        }

        tracing::info!("st: {:?}", rx0.borrow());
    }
    assert!(rx0.changed().await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_leader_and_non_voter() -> anyhow::Result<()> {
    // - Start a leader and a non-voter;
    // - Write to leader, check on non-voter.

    common_tracing::init_default_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?;

    assert_set_file_synced(vec![mn0.clone(), mn1.clone()], "metakey2").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_write_to_local_leader() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and a non-voter;
    // - Write to the raft node on the leader, expect Ok.
    // - Write to the raft node on the non-leader, expect ForwardToLeader error.

    common_tracing::init_default_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?; // follower
    let (_nid2, mn2) = setup_non_voter(mn0.clone(), 2).await?; // follower
    let (_nid3, mn3) = setup_non_voter(mn0.clone(), 3).await?; // non-voter

    mn0.raft.change_membership(hashset![0, 1, 2]).await?;

    let all = vec![mn0.clone(), mn1.clone(), mn2.clone(), mn3.clone()];

    // ensure cluster works
    assert_set_file_synced(all.clone(), "foo").await?;

    let leader_id = mn0.raft.metrics().borrow().current_leader.unwrap();

    // test writing to leader and non-leader
    let key = "t-non-leader-write";
    for id in 0u64..4 {
        let mn = &all[id as usize];
        let rst = mn
            .write_to_local_leader(LogEntry {
                txid: None,
                cmd: Cmd::SetFile {
                    key: key.to_string(),
                    value: key.to_string(),
                },
            })
            .await;

        let rst = rst?;

        if id == leader_id {
            assert!(rst.is_ok());
        } else {
            assert!(rst.is_err());
            let e = rst.unwrap_err();
            match e {
                RetryableError::ForwardToLeader { leader } => {
                    assert_eq!(leader_id, leader);
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_set_file() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and a non-voter;
    // - Write to the raft node on every node, expect Ok.

    // TODO: test MetaNode.write during leader changes.

    common_tracing::init_default_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?; // follower
    let (_nid2, mn2) = setup_non_voter(mn0.clone(), 2).await?; // follower
    let (_nid3, mn3) = setup_non_voter(mn0.clone(), 3).await?; // non-voter

    mn0.raft.change_membership(hashset![0, 1, 2]).await?;

    let all = vec![mn0.clone(), mn1.clone(), mn2.clone(), mn3.clone()];

    // ensure cluster works
    assert_set_file_synced(all.clone(), "foo").await?;

    // test writing
    for id in 0u64..4 {
        let key = format!("t-write-{}", id);
        let mn = &all[id as usize];

        let last_applied = mn.raft.metrics().borrow().last_applied;

        let rst = mn
            .write(LogEntry {
                txid: None,
                cmd: Cmd::SetFile {
                    key: key.to_string(),
                    value: key.to_string(),
                },
            })
            .await;

        assert!(rst.is_ok());

        assert_applied_index(all.clone(), last_applied + 1).await?;
        assert_get_file(all.clone(), &key, &key).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_add_database() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and a non-voter;
    // - Assert that every node handles AddDatabase request correctly.

    common_tracing::init_default_tracing();

    let all = setup_cluster(hashset![0, 1, 2], hashset![3]).await?;

    // ensure cluster works
    assert_set_file_synced(all.clone(), "foo").await?;

    // - db name to create
    // - expected db id
    let cases: Vec<(&str, u64)> = vec![("foo", 1), ("bar", 2), ("foo", 1), ("bar", 2)];

    // Sending AddDatabase request to any node is ok.
    for (i, (name, want_id)) in cases.iter().enumerate() {
        let mn = &all[i as usize];

        let last_applied = mn.raft.metrics().borrow().last_applied;

        let rst = mn
            .write(LogEntry {
                txid: None,
                cmd: Cmd::AddDatabase {
                    name: name.to_string(),
                },
            })
            .await;

        assert!(rst.is_ok());

        // No matter if a db is created, the log that tries to create db always applies.
        assert_applied_index(all.clone(), last_applied + 1).await?;

        for (i, mn) in all.iter().enumerate() {
            let got = mn.get_database(&name).await;

            assert_eq!(
                *want_id,
                got.unwrap().database_id,
                "n{} applied AddDatabase",
                i
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_meta_node_3_members() -> anyhow::Result<()> {
    // - Bring a leader online.
    // - Add 2 node to the cluster.
    // - Write to leader, check data is replicated.

    common_tracing::init_default_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?;
    let (_nid2, mn2) = setup_non_voter(mn0.clone(), 2).await?;

    let mut nlog = 1 + 3; // leader add a blank log, adding a node commits one log.

    nlog += assert_set_file_synced(vec![mn0.clone(), mn1.clone(), mn2.clone()], "foo-1").await?;

    // add node 1 and 2 as follower
    mn0.raft.change_membership(hashset![0, 1, 2]).await?;
    nlog += 2; // joint consensus commits 2 logs

    wait_for_state(&mn1, State::Follower).await?;
    wait_for_state(&mn2, State::Follower).await?;
    wait_for_state(&mn0, State::Leader).await?;

    wait_for_log(&mn0, nlog).await?;
    wait_for_log(&mn1, nlog).await?;
    wait_for_log(&mn2, nlog).await?;

    assert_set_file_synced(vec![mn0.clone(), mn1.clone(), mn2.clone()], "foo-2").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_restart() -> anyhow::Result<()> {
    // TODO check restarted follower.
    // - Start a leader and a non-voter;
    // - Restart them.
    // - Check old data an new written data.

    common_tracing::init_default_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?;
    let sto0 = mn0.sto.clone();
    let sto1 = mn1.sto.clone();

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    assert_set_file_synced(meta_nodes.clone(), "key1").await?;

    // stop
    tracing::info!("shutting down all");

    let n = mn0.stop().await?;
    assert_eq!(3, n);
    let n = mn1.stop().await?;
    assert_eq!(3, n);

    tracing::info!("restart all");

    // restart
    let mn0 = MetaNode::builder().node_id(0).sto(sto0).build().await?;
    let mn1 = MetaNode::builder().node_id(1).sto(sto1).build().await?;

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    wait_for_state(&mn0, State::Leader).await?;
    wait_for_state(&mn1, State::NonVoter).await?;
    wait_for_current_leader(&mn1, 0).await?;

    assert_set_file_synced(meta_nodes.clone(), "key2").await?;

    // check old data
    assert_get_file(meta_nodes, "key1", "key1").await?;

    Ok(())
}

/// Setup a cluster with several voter and several non_voter
/// The node id 0 must be in `voters` and node 0 is elected as leader.
async fn setup_cluster(
    voters: HashSet<NodeId>,
    non_voters: HashSet<NodeId>,
) -> anyhow::Result<Vec<Arc<MetaNode>>> {
    // leader is always node-0
    assert!(voters.contains(&0));
    assert!(!non_voters.contains(&0));

    let mut rst = vec![];

    let (_id, mn) = setup_leader().await?;
    rst.push(mn.clone());
    let leader = mn;

    for id in voters.iter() {
        // leader is already created.
        if *id == 0 {
            continue;
        }
        let (_id, mn) = setup_non_voter(leader.clone(), *id).await?;
        rst.push(mn);
    }

    for id in non_voters.iter() {
        let (_id, mn) = setup_non_voter(leader.clone(), *id).await?;
        rst.push(mn);
    }

    leader.raft.change_membership(voters).await?;

    Ok(rst)
}

async fn setup_leader() -> anyhow::Result<(NodeId, Arc<MetaNode>)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    let nid = 0;
    let addr = new_addr();

    // boot up a single-node cluster
    let mn = MetaNode::boot(nid, addr.clone()).await?;

    {
        assert_meta_connection(&addr).await?;

        // assert that boot() adds the node to meta.
        let got = mn.get_node(&nid).await;
        assert_eq!(addr, got.unwrap().address, "nid0 is added");

        wait_for_state(&mn, State::Leader).await?;
        wait_for_current_leader(&mn, 0).await?;
    }
    Ok((nid, mn))
}

/// Start a NonVoter and setup replication from leader to it.
/// Assert the NonVoter is ready and upto date such as the known leader, state and grpc service.
async fn setup_non_voter(
    leader: Arc<MetaNode>,
    id: NodeId,
) -> anyhow::Result<(NodeId, Arc<MetaNode>)> {
    let addr = new_addr();

    let mn = MetaNode::boot_non_voter(id, &addr).await?;

    {
        // add node to cluster as a non-voter
        let resp = leader.add_node(id, addr.clone()).await?;
        match resp {
            AppliedState::Node { prev: _, result } => {
                assert_eq!(addr.clone(), result.unwrap().address);
            }
            _ => {
                panic!("expect node")
            }
        }
    }

    {
        assert_meta_connection(&addr).await?;
        wait_for_state(&mn, State::NonVoter).await?;
        wait_for_current_leader(&mn, 0).await?;
    }

    Ok((id, mn))
}

/// Write one log on leader, check all nodes replicated the log.
/// Returns the number log committed.
async fn assert_set_file_synced(meta_nodes: Vec<Arc<MetaNode>>, key: &str) -> anyhow::Result<u64> {
    let leader = meta_nodes[0].clone();

    let last_applied = leader.raft.metrics().borrow().last_applied;
    tracing::info!("leader: last_applied={}", last_applied);
    {
        leader
            .write_to_local_leader(LogEntry {
                txid: None,
                cmd: Cmd::SetFile {
                    key: key.to_string(),
                    value: key.to_string(),
                },
            })
            .await??;
    }

    assert_applied_index(meta_nodes.clone(), last_applied + 1).await?;
    assert_get_file(meta_nodes.clone(), key, key).await?;

    Ok(1)
}

/// Wait nodes for applied index to be upto date: applied >= at_least.
async fn assert_applied_index(meta_nodes: Vec<Arc<MetaNode>>, at_least: u64) -> anyhow::Result<()> {
    for (_i, mn) in meta_nodes.iter().enumerate() {
        wait_for_log(&mn, at_least).await?;
    }
    Ok(())
}

async fn assert_get_file(
    meta_nodes: Vec<Arc<MetaNode>>,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    for (i, mn) in meta_nodes.iter().enumerate() {
        let got = mn.get_file(key).await;
        assert_eq!(value.to_string(), got.unwrap(), "n{} applied value", i);
    }
    Ok(())
}

/// Wait for the known leader of a raft to become the expected `leader_id` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(mn))]
pub async fn wait_for_current_leader(
    mn: &MetaNode,
    leader_id: NodeId,
) -> anyhow::Result<RaftMetrics> {
    let metrics = mn
        .raft
        .wait(timeout())
        .current_leader(leader_id, "")
        .await?;
    Ok(metrics)
}

/// Wait for raft log to become the expected `index` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(mn))]
async fn wait_for_log(mn: &MetaNode, index: u64) -> anyhow::Result<RaftMetrics> {
    let metrics = mn.raft.wait(timeout()).log(index, "").await?;
    Ok(metrics)
}

/// Wait for raft state to become the expected `state` until a default 2000 ms time out.
#[tracing::instrument(level = "debug", skip(mn))]
pub async fn wait_for_state(
    mn: &MetaNode,
    state: async_raft::State,
) -> anyhow::Result<RaftMetrics> {
    let metrics = mn.raft.wait(timeout()).state(state, "").await?;
    Ok(metrics)
}

/// Wait for raft metrics to become a state that satisfies `func`.
#[tracing::instrument(level = "debug", skip(mn, func))]
async fn wait_for<T>(mn: &MetaNode, func: T) -> anyhow::Result<RaftMetrics>
where T: Fn(&RaftMetrics) -> bool + Send {
    let metrics = mn.raft.wait(timeout()).metrics(func, "").await?;
    Ok(metrics)
}

/// Make a default timeout for wait() for test.
fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(2000))
}

fn new_addr() -> String {
    let addr = format!("127.0.0.1:{}", 19000 + *Seq::default());
    tracing::info!("new_addr: {}", addr);
    addr
}
