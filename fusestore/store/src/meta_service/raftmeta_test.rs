// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;
use crate::meta_service::Node;
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

    let mn = MetaNode::new(0).await;

    let addr = "127.0.0.1:9000".to_string();
    let resp = mn.boot(addr.clone()).await;
    assert!(resp.is_ok());

    let got = mn.get_node(&0).await;
    assert_eq!(addr, got.unwrap().address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_add_non_voter() -> anyhow::Result<()> {
    crate::tests::init_tracing();

    // Start a meta service cluster with one voter(leader) and one non-voter.

    // node-0: voter
    let nid0 = 0;
    let addr0 = "127.0.0.1:19000".to_string();
    let mn0 = MetaNode::new(nid0).await;
    let mut mrx0 = mn0.raft.metrics();
    let srv0 = MetaServiceImpl::create(mn0.clone()).await;
    let srv0 = MetaServiceServer::new(srv0);
    serve_grpc!(addr0, srv0);

    check_connection(addr0.clone()).await?;

    // node-1: non-voter
    let nid1 = 1;
    let addr1 = "127.0.0.1:19001".to_string();
    let mn1 = MetaNode::new(1).await;
    let mut mrx1 = mn1.raft.metrics();
    let srv1 = MetaServiceImpl::create(mn1.clone()).await;
    let srv1 = MetaServiceServer::new(srv1);
    serve_grpc!(addr1, srv1);

    check_connection(addr1.clone()).await?;

    wait_for("nid0 to be non-voter", &mut mrx0, |x| {
        x.state == State::NonVoter
    })
    .await;
    wait_for("nid1 to be non-voter", &mut mrx1, |x| {
        x.state == State::NonVoter
    })
    .await;

    {
        // boot up a single-node cluster
        let resp = mn0.boot(addr0.clone()).await;
        assert!(resp.is_ok());

        let got = mn0.get_node(&nid0).await;
        assert_eq!(addr0, got.unwrap().address, "nid1 is added");

        wait_for("nid0 to be leader", &mut mrx0, |x| x.state == State::Leader).await;
        wait_for("nid0 current_leader==0", &mut mrx0, |x| {
            x.current_leader == Some(0)
        })
        .await;
    }

    {
        // add node-1 to cluster as non-voter
        let resp = mn0
            .write_to_local_leader(ClientRequest {
                txid: None,
                cmd: Cmd::AddNode {
                    node_id: nid1,
                    node: Node {
                        name: "".to_string(),
                        address: addr1.clone()
                    }
                }
            })
            .await;
        match resp.unwrap() {
            ClientResponse::Node { prev: _, result } => {
                assert_eq!(addr1, result.unwrap().address);
            }
            _ => {
                panic!("expect node")
            }
        }
    }

    wait_for("nid1 current_leader==0", &mut mrx1, |x| {
        x.current_leader == Some(0)
    })
    .await;

    Ok(())
}

async fn check_connection(addr: String) -> anyhow::Result<()> {
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
        tracing::info!("wait for {:} metrics: {:?}", msg, latest);
        if func(&latest) {
            return latest;
        }

        let changed = mrx.changed().await;
        assert!(changed.is_ok());
    }
}
