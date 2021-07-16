// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::State;
use common_runtime::tokio;
#[allow(unused_imports)]
use log::info;
use pretty_assertions::assert_eq;

use crate::meta_service::AppliedState;
use crate::meta_service::Cmd;
use crate::meta_service::GetReq;
use crate::meta_service::LogEntry;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::RetryableError;
use crate::tests::assert_meta_connection;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_add_file() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let tc = new_test_context();
    let addr = tc.config.meta_api_addr();

    let _mn = MetaNode::boot(0, &tc.config).await?;
    assert_meta_connection(&addr).await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    let cases = crate::meta_service::raftmeta_test::cases_add_file();

    for (name, txid, k, v, want_prev, want_rst) in cases.iter() {
        let req = LogEntry {
            txid: txid.clone(),
            cmd: Cmd::AddFile {
                key: k.to_string(),
                value: v.to_string(),
            },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::String { prev, result } => {
                assert_eq!(*want_prev, prev, "{}", name);
                assert_eq!(*want_rst, result, "{}", name);
            }
            _ => {
                panic!("not String")
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_set_file() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let tc = new_test_context();
    let addr = tc.config.meta_api_addr();

    let _mn = MetaNode::boot(0, &tc.config).await?;
    assert_meta_connection(&addr).await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    let cases = crate::meta_service::raftmeta_test::cases_set_file();

    for (name, txid, k, v, want_prev, want_rst) in cases.iter() {
        let req = LogEntry {
            txid: txid.clone(),
            cmd: Cmd::SetFile {
                key: k.to_string(),
                value: v.to_string(),
            },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::String { prev, result } => {
                assert_eq!(*want_prev, prev, "{}", name);
                assert_eq!(*want_rst, result, "{}", name);
            }
            _ => {
                panic!("not String")
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_add_set_get() -> anyhow::Result<()> {
    // Test Cmd::AddFile, Cmd::SetFile, Cma::GetFile

    common_tracing::init_default_tracing();

    let tc = new_test_context();
    let addr = tc.config.meta_api_addr();

    let _mn = MetaNode::boot(0, &tc.config).await?;
    assert_meta_connection(&addr).await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    {
        // add: ok
        let req = LogEntry {
            txid: None,
            cmd: Cmd::AddFile {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::String { prev: _, result } => {
                assert_eq!("bar".to_string(), result.unwrap());
            }
            _ => {
                panic!("not string")
            }
        }

        // get the stored value

        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("bar", rst.value);
    }

    {
        // add: conflict with existent.
        let req = LogEntry {
            txid: None,
            cmd: Cmd::AddFile {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::String { prev: _, result } => {
                assert!(result.is_none());
            }
            _ => {
                panic!("not string")
            }
        }
    }
    {
        // set: override. ok.
        let req = LogEntry {
            txid: None,
            cmd: Cmd::SetFile {
                key: "foo".to_string(),
                value: "bar2".to_string(),
            },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::String { prev: _, result } => {
                assert_eq!(Some("bar2".to_string()), result);
            }
            _ => {
                panic!("not string")
            }
        }

        // get the stored value

        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("bar2", rst.value);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_incr_seq() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let tc = new_test_context();
    let addr = tc.config.meta_api_addr();

    let _mn = MetaNode::boot(0, &tc.config).await?;
    assert_meta_connection(&addr).await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    let cases = crate::meta_service::raftmeta_test::cases_incr_seq();

    for (name, txid, k, want) in cases.iter() {
        let req = LogEntry {
            txid: txid.clone(),
            cmd: Cmd::IncrSeq { key: k.to_string() },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::Seq { seq } => {
                assert_eq!(*want, seq, "{}", name);
            }
            _ => {
                panic!("not Seq")
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_cluster_write_on_non_leader() -> anyhow::Result<()> {
    // - Bring up a cluster of one leader and one non-voter
    // - Assert that writing on the non-voter returns ForwardToLeader error

    common_tracing::init_default_tracing();

    let tc0 = new_test_context();
    let tc1 = new_test_context();

    let addr0 = tc0.config.meta_api_addr();
    let addr1 = tc1.config.meta_api_addr();

    let mn0 = MetaNode::boot(0, &tc0.config).await?;
    assert_meta_connection(&addr0).await?;

    {
        // add node 1 as non-voter
        let mn1 = MetaNode::boot_non_voter(1, &tc1.config).await?;
        assert_meta_connection(&addr0).await?;

        let resp = mn0.add_node(1, addr1.clone()).await?;
        match resp {
            AppliedState::Node { prev: _, result } => {
                assert_eq!(addr1.clone(), result.unwrap().address);
            }
            _ => {
                panic!("expect node")
            }
        }
        mn1.raft.wait(None).state(State::NonVoter, "").await?;
        mn1.raft.wait(None).current_leader(0, "").await?;
    }

    let mut client = MetaServiceClient::connect(format!("http://{}", addr1)).await?;

    let req = LogEntry {
        txid: None,
        cmd: Cmd::SetFile {
            key: "t-write-on-non-voter".to_string(),
            value: "t-write-on-non-voter".to_string(),
        },
    };
    let raft_mes = client.write(req).await?.into_inner();

    let rst: Result<AppliedState, RetryableError> = raft_mes.into();
    assert!(rst.is_err());
    let err = rst.unwrap_err();
    match err {
        RetryableError::ForwardToLeader { leader } => {
            assert_eq!(leader, 0);
        }
    }

    Ok(())
}
