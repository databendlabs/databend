// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::RaftMetrics;
use async_raft::State;
use pretty_assertions::assert_eq;
use tokio::sync::watch::Receiver;

use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_node_boot() -> anyhow::Result<()> {
    crate::tests::init_tracing();

    // Start a single node meta service cluster.
    // Test the single node is recorded by this cluster.

    let mn = MetaNode::new(0).await;

    let addr = "127.0.0.1:9000".to_string();
    let resp = mn.boot(addr.clone()).await;
    assert!(resp.is_ok());

    let got = mn.get(mn.sto.node_key(0)).await;
    assert_eq!(addr, got.unwrap());
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

        let got = mn0.get(mn0.sto.node_key(nid0)).await;
        assert_eq!(addr0, got.unwrap(), "nid1 is added");

        wait_for("nid0 to be leader", &mut mrx0, |x| x.state == State::Leader).await;
        wait_for("nid0 current_leader==0", &mut mrx0, |x| {
            x.current_leader == Some(0)
        })
        .await;
    }

    {
        // add node-1 to cluster as non-voter
        let key = mn0.sto.node_key(nid1);
        let resp = mn0.local_set(key, addr1.clone()).await;
        assert_eq!(addr1, resp.unwrap());
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
