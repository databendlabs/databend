// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_raft::RaftMetrics;
use async_raft::State;
use pretty_assertions::assert_eq;
use tokio::sync::watch::Receiver;

use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::GetReq;
use crate::meta_service::MemStoreStateMachine;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::NodeId;
use crate::meta_service::RaftTxId;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_add() -> anyhow::Result<()> {
    crate::tests::init_tracing();

    let mut sm = MemStoreStateMachine::default();
    let cases: Vec<(
        &str,
        Option<RaftTxId>,
        &str,
        &str,
        Option<String>,
        Option<String>
    )> = vec![
        (
            "add on none",
            Some(RaftTxId::new("foo", 1)),
            "k1",
            "v1",
            None,
            Some("v1".to_string())
        ),
        (
            "add on existent",
            Some(RaftTxId::new("foo", 2)),
            "k1",
            "v2",
            Some("v1".to_string()),
            None
        ),
        (
            "dup set with same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            "v3",
            Some("v1".to_string()),
            None
        ),
        (
            "diff client, same serial",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            "v3",
            None,
            Some("v3".to_string())
        ),
        ("no txid", None, "k3", "v4", None, Some("v4".to_string())),
    ];
    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm.apply(5, &ClientRequest {
            txid: txid.clone(),
            cmd: Cmd::AddFile {
                key: k.to_string(),
                value: v.to_string()
            }
        });
        assert_eq!(
            ClientResponse::String {
                prev: want_prev.clone(),
                result: want_result.clone()
            },
            resp.unwrap(),
            "{}",
            name
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_set() -> anyhow::Result<()> {
    crate::tests::init_tracing();

    let mut sm = MemStoreStateMachine::default();
    let cases: Vec<(
        &str,
        Option<RaftTxId>,
        &str,
        &str,
        Option<String>,
        Option<String>
    )> = vec![
        (
            "set on none",
            Some(RaftTxId::new("foo", 1)),
            "k1",
            "v1",
            None,
            Some("v1".to_string())
        ),
        (
            "set on existent",
            Some(RaftTxId::new("foo", 2)),
            "k1",
            "v2",
            Some("v1".to_string()),
            Some("v2".to_string())
        ),
        (
            "dup set with same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            "v3",
            Some("v1".to_string()),
            Some("v2".to_string())
        ),
        (
            "diff client, same serial",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            "v3",
            None,
            Some("v3".to_string())
        ),
        (
            "no txid",
            None,
            "k2",
            "v4",
            Some("v3".to_string()),
            Some("v4".to_string())
        ),
    ];
    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm.apply(5, &ClientRequest {
            txid: txid.clone(),
            cmd: Cmd::SetFile {
                key: k.to_string(),
                value: v.to_string()
            }
        });
        assert_eq!(
            ClientResponse::String {
                prev: want_prev.clone(),
                result: want_result.clone()
            },
            resp.unwrap(),
            "{}",
            name
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_boot() -> anyhow::Result<()> {
    crate::tests::init_tracing();

    // Start a single node meta service cluster.
    // Test the single node is recorded by this cluster.

    let addr = "127.0.0.1:9000".to_string();
    let resp = MetaNode::boot(0, addr.clone()).await;
    assert!(resp.is_ok());

    let mn = resp.unwrap();

    let got = mn.get_node(&0).await;
    assert_eq!(addr, got.unwrap().address);
    mn.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_add_non_voter() -> anyhow::Result<()> {
    crate::tests::init_tracing();

    // Start a meta service cluster with one voter(leader) and one non-voter.
    let (mn0, mn1) = setup_leader_non_voter().await?;
    mn0.stop().await?;
    mn1.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_graceful_shutdown() -> anyhow::Result<()> {
    // Start a leader then shutdown.

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
    // Start a leader and a non-voter;
    // Write to leader, check on non-voter.

    crate::tests::init_tracing();
    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1, "127.0.0.1:19007").await?;
    check_write_synced(vec![mn0.clone(), mn1.clone()], "metakey2").await?;

    mn0.stop().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_restart() -> anyhow::Result<()> {
    // TODO check restarted follower.
    // Start a leader and a non-voter;
    // Restart them.
    // Check old data an new written data.

    crate::tests::init_tracing();

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1, "127.0.0.1:19008").await?;
    let sto0 = mn0.sto.clone();
    let sto1 = mn1.sto.clone();

    check_write_synced(vec![mn0.clone(), mn1.clone()], "key1").await?;

    // stop
    tracing::info!("shutting down all");

    let n = mn0.stop().await?;
    assert_eq!(3, n);
    let n = mn1.stop().await?;
    assert_eq!(3, n);

    // restart
    let mn0 = MetaNode::builder().node_id(0).sto(sto0).build().await?;
    let mut rx0 = mn0.raft.metrics();
    let mn1 = MetaNode::builder().node_id(1).sto(sto1).build().await?;
    let mut rx1 = mn1.raft.metrics();

    // TODO check old data
    wait_for("n0 -> leader", &mut rx0, |x| x.state == State::Leader).await;
    wait_for("n1 -> non-voter", &mut rx1, |x| x.state == State::NonVoter).await;

    wait_for("n1.current_leader -> 0", &mut rx1, |x| {
        x.current_leader == Some(0)
    })
    .await;

    check_write_synced(vec![mn0.clone(), mn1.clone()], "key2").await?;

    Ok(())
}

async fn setup_leader_non_voter() -> anyhow::Result<(Arc<MetaNode>, Arc<MetaNode>)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    let (_nid0, mn0) = setup_leader().await?;
    let (_nid1, mn1) = setup_non_voter(mn0.clone(), 1, "127.0.0.1:19001").await?;

    Ok((mn0, mn1))
}
async fn setup_leader() -> anyhow::Result<(NodeId, Arc<MetaNode>)> {
    // Setup a cluster in which there is a leader and a non-voter.
    // asserts states are consistent

    // node-0: voter, becomes leader.
    let nid0 = 0;
    let addr0 = "127.0.0.1:19000".to_string();

    // boot up a single-node cluster
    let mn0 = MetaNode::boot(nid0, addr0.clone()).await?;
    let mut rx0 = mn0.raft.metrics();

    {
        // ensure n0 is ready
        check_connection(&addr0).await?;

        // assert that boot() adds the node to meta.
        let got = mn0.get_node(&nid0).await;
        assert_eq!(addr0, got.unwrap().address, "nid0 is added");

        wait_for("n0 -> leader", &mut rx0, |x| x.state == State::Leader).await;
        wait_for("n0.current_leader -> 0", &mut rx0, |x| {
            x.current_leader == Some(0)
        })
        .await;
    }
    Ok((nid0, mn0))
}

async fn setup_non_voter(
    leader: Arc<MetaNode>,
    id: NodeId,
    addr: &str
) -> anyhow::Result<(NodeId, Arc<MetaNode>)> {
    let mn1 = MetaNode::boot_non_voter(id, addr).await?;
    let mut rx1 = mn1.raft.metrics();

    {
        // add node-1 to cluster as non-voter
        let resp = leader.add_node(id, addr.to_string()).await;
        match resp.unwrap() {
            ClientResponse::Node { prev: _, result } => {
                assert_eq!(addr.to_string(), result.unwrap().address);
            }
            _ => {
                panic!("expect node")
            }
        }
    }

    {
        // ensure n1 is ready
        check_connection(addr).await?;
        wait_for(&format!("n{} -> non-voter", id), &mut rx1, |x| {
            x.state == State::NonVoter
        })
        .await;
        wait_for(&format!("n{}.current_leader -> 0", id), &mut rx1, |x| {
            x.current_leader == Some(0)
        })
        .await;
    }

    Ok((id, mn1))
}

async fn check_write_synced(meta_nodes: Vec<Arc<MetaNode>>, key: &str) -> anyhow::Result<()> {
    let leader = meta_nodes[0].clone();

    let last_applied = leader.raft.metrics().borrow().last_applied;
    tracing::info!("leader: last_applied={}", last_applied);
    {
        // write a 2nd key to leader
        leader
            .write_to_local_leader(ClientRequest {
                txid: None,
                cmd: Cmd::SetFile {
                    key: key.to_string(),
                    value: key.to_string()
                }
            })
            .await?;
    }

    // wait for leader and others for applied index upto date
    for i in 0..meta_nodes.len() {
        let mn = meta_nodes[i].clone();
        // raft.metrics is the status of the cluster, not the status about a node.
        // E.g., if leader applied 4th log, the next append_entry request updates the applied index to 4 on a follower or non-voter,
        // no matter whether it actually applied the 4th log.
        // Thus we check the applied_rx, which is the actually applied index.
        wait_for_applied(
            &format!("n{}", i,),
            &mut mn.sto.applied_rx.clone(),
            last_applied + 1
        )
        .await;
    }

    // check data written on leader and all other nodes
    for i in 0..meta_nodes.len() {
        let mn = meta_nodes[i].clone();
        let val = mn.get_file(key).await;
        assert_eq!(key.to_string(), val.unwrap(), "n{} applied value", i);
    }

    Ok(())
}

async fn check_connection(addr: &str) -> anyhow::Result<()> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
    let req = tonic::Request::new(GetReq { key: "foo".into() });
    let rst = client.get(req).await?.into_inner();
    assert_eq!("", rst.value, "connected");
    Ok(())
}

// wait for raft metrics to a state that satisfies `func`.
#[tracing::instrument(level = "info", skip(func, mrx))]
async fn wait_for<T>(msg: &str, mrx: &mut Receiver<RaftMetrics>, func: T) -> RaftMetrics
where T: Fn(&RaftMetrics) -> bool {
    loop {
        let latest = mrx.borrow().clone();
        tracing::info!("start wait for {:} metrics: {:?}", msg, latest);
        if func(&latest) {
            tracing::info!("done  wait for {:} metrics: {:?}", msg, latest);
            return latest;
        }

        let changed = mrx.changed().await;
        assert!(changed.is_ok());
    }
}

// wait for the applied index to be >= `at_least`.
#[tracing::instrument(level = "info", skip(rx))]
async fn wait_for_applied(msg: &str, rx: &mut Receiver<u64>, at_least: u64) -> u64 {
    loop {
        let latest = rx.borrow().clone();
        tracing::info!("start wait for {:} latest: {:?}", msg, latest);
        if latest >= at_least {
            tracing::info!("done  wait for {:} latest: {:?}", msg, latest);
            return latest;
        }

        let changed = rx.changed().await;
        assert!(changed.is_ok());
    }
}
