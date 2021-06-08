// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_raft::RaftMetrics;
use async_raft::State;
use maplit::hashset;
use pretty_assertions::assert_eq;
use tokio::sync::watch::Receiver;
use tokio::time::Duration;

use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::NodeId;
use crate::meta_service::RaftTxId;
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

    crate::tests::init_tracing();

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

    crate::tests::init_tracing();

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
async fn test_meta_node_sync_to_non_voter() -> anyhow::Result<()> {
    // - Start a leader and a non-voter;
    // - Write to leader, check on non-voter.

    crate::tests::init_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?;

    assert_set_file_synced(vec![mn0.clone(), mn1.clone()], "metakey2").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_meta_node_3_members() -> anyhow::Result<()> {
    // - Bring a leader online.
    // - Add 2 node to the cluster.
    // - Write to leader, check data is replicated.

    crate::tests::init_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1).await?;
    let (_nid2, mn2) = setup_non_voter(mn0.clone(), 2).await?;

    let mut nlog = 1 + 3; // leader add a blank log, adding a node commits one log.

    nlog += assert_set_file_synced(vec![mn0.clone(), mn1.clone(), mn2.clone()], "foo-1").await?;

    // add node 1 and 2 as follower
    mn0.raft.change_membership(hashset![0, 1, 2]).await?;
    nlog += 2; // joint consensus commits 2 logs

    wait_for_state(1, &mut mn1.raft.metrics(), State::Follower).await?;
    wait_for_state(2, &mut mn2.raft.metrics(), State::Follower).await?;
    wait_for_state(0, &mut mn0.raft.metrics(), State::Leader).await?;

    wait_for_log(0, &mut mn0.raft.metrics(), nlog).await?;
    wait_for_log(1, &mut mn1.raft.metrics(), nlog).await?;
    wait_for_log(2, &mut mn2.raft.metrics(), nlog).await?;

    assert_set_file_synced(vec![mn0.clone(), mn1.clone(), mn2.clone()], "foo-2").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_restart() -> anyhow::Result<()> {
    // TODO check restarted follower.
    // - Start a leader and a non-voter;
    // - Restart them.
    // - Check old data an new written data.

    crate::tests::init_tracing();

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
    let mut rx0 = mn0.raft.metrics();
    let mn1 = MetaNode::builder().node_id(1).sto(sto1).build().await?;
    let mut rx1 = mn1.raft.metrics();

    let meta_nodes = vec![mn0.clone(), mn1.clone()];

    wait_for_state(0, &mut rx0, State::Leader).await?;
    wait_for_state(1, &mut rx1, State::NonVoter).await?;
    wait_for_current_leader(1, &mut rx1, 0).await?;

    assert_set_file_synced(meta_nodes.clone(), "key2").await?;

    // check old data
    assert_get_file(meta_nodes, "key1", "key1").await?;

    Ok(())
}

async fn setup_leader() -> anyhow::Result<(NodeId, Arc<MetaNode>)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    let nid = 0;
    let addr = new_addr();

    // boot up a single-node cluster
    let mn = MetaNode::boot(nid, addr.clone()).await?;
    let mut rx = mn.raft.metrics();

    {
        assert_connection(&addr).await?;

        // assert that boot() adds the node to meta.
        let got = mn.get_node(&nid).await;
        assert_eq!(addr, got.unwrap().address, "nid0 is added");

        wait_for_state(0, &mut rx, State::Leader).await?;
        wait_for_current_leader(0, &mut rx, 0).await?;
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
    let mut rx = mn.raft.metrics();

    {
        // add node to cluster as a non-voter
        let resp = leader.add_node(id, addr.clone()).await?;
        match resp {
            ClientResponse::Node { prev: _, result } => {
                assert_eq!(addr.clone(), result.unwrap().address);
            }
            _ => {
                panic!("expect node")
            }
        }
    }

    {
        assert_connection(&addr).await?;
        wait_for_state(id, &mut rx, State::NonVoter).await?;
        wait_for_current_leader(id, &mut rx, 0).await?;
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
            .write_to_local_leader(ClientRequest {
                txid: None,
                cmd: Cmd::SetFile {
                    key: key.to_string(),
                    value: key.to_string(),
                },
            })
            .await?;
    }

    assert_applied_index(meta_nodes.clone(), last_applied + 1).await?;
    assert_get_file(meta_nodes.clone(), key, key).await?;

    Ok(1)
}

/// Wait nodes for applied index to be upto date: applied >= at_least.
async fn assert_applied_index(meta_nodes: Vec<Arc<MetaNode>>, at_least: u64) -> anyhow::Result<()> {
    for (i, mn) in meta_nodes.iter().enumerate() {
        let mut rx = mn.raft.metrics();
        wait_for_log(i, &mut rx, at_least).await?;
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

pub async fn assert_connection(addr: &str) -> anyhow::Result<()> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
    let req = tonic::Request::new(GetReq { key: "foo".into() });
    let rst = client.get(req).await?.into_inner();
    assert_eq!("", rst.value, "connected");
    Ok(())
}

/// Wait for the known leader of a raft to become the expected `leader_id` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(msg,rx), fields(msg=msg.to_string().as_str()))]
async fn wait_for_current_leader(
    msg: impl ToString,
    rx: &mut Receiver<RaftMetrics>,
    leader_id: NodeId,
) -> anyhow::Result<RaftMetrics> {
    wait_for(
        format!("{}: current_leader -> {}", msg.to_string(), leader_id),
        rx,
        |x| x.current_leader == Some(leader_id),
    )
    .await
}

/// Wait for raft log to become the expected `index` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(msg,rx), fields(msg=msg.to_string().as_str()))]
async fn wait_for_log(
    msg: impl ToString,
    rx: &mut Receiver<RaftMetrics>,
    index: u64,
) -> anyhow::Result<RaftMetrics> {
    wait_for(
        format!("{}: last_log_index -> {}", msg.to_string(), index),
        rx,
        |x| x.last_log_index == index,
    )
    .await?;
    wait_for(
        format!("{}: last_applied -> {}", msg.to_string(), index),
        rx,
        |x| x.last_applied == index,
    )
    .await
}

/// Wait for raft state to become the expected `state` until a default 2000 ms time out.
#[tracing::instrument(level = "info", skip(msg,rx), fields(msg=msg.to_string().as_str()))]
async fn wait_for_state(
    msg: impl ToString,
    rx: &mut Receiver<RaftMetrics>,
    state: async_raft::State,
) -> anyhow::Result<RaftMetrics> {
    wait_for(
        format!("{}: state -> {:?}", msg.to_string(), state),
        rx,
        |x| x.state == state,
    )
    .await
}

/// Same as wait_for_with_timeout except it use a default timeout 2000 ms.
#[tracing::instrument(level = "info", skip(msg,rx,func), fields(msg=msg.to_string().as_str()))]
async fn wait_for<T>(
    msg: impl ToString,
    rx: &mut Receiver<RaftMetrics>,
    func: T,
) -> anyhow::Result<RaftMetrics>
where
    T: Fn(&RaftMetrics) -> bool,
{
    let timeout = Duration::from_millis(2000);
    wait_for_with_timeout(msg, rx, func, timeout).await
}

/// Wait for raft metrics to become a state that satisfies `func`,
/// until the specified timeout.
#[tracing::instrument(level = "info", skip(msg,rx,func), fields(msg=msg.to_string().as_str()))]
async fn wait_for_with_timeout<T>(
    msg: impl ToString,
    rx: &mut Receiver<RaftMetrics>,
    func: T,
    timeout: Duration,
) -> anyhow::Result<RaftMetrics>
where
    T: Fn(&RaftMetrics) -> bool,
{
    loop {
        let latest = rx.borrow().clone();

        tracing::info!("start wait for {:} latest: {:?}", msg.to_string(), latest);
        if func(&latest) {
            tracing::info!("done wait for {:} latest: {:?}", msg.to_string(), latest);
            return Ok(latest);
        }

        let delay = tokio::time::sleep(timeout);
        tokio::select! {
            _ = delay => {
                return Err(anyhow::anyhow!("timeout wait for {} latest: {:?}", msg.to_string(), latest));
            }
            changed = rx.changed() => {
                changed?;
            }
        };
    }
}

fn new_addr() -> String {
    let addr = format!("127.0.0.1:{}", 19000 + *Seq::default());
    tracing::info!("new_addr: {}", addr);
    addr
}
