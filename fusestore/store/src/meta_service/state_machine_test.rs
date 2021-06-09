// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;

use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::MemStoreStateMachine;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_add_file() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut sm = MemStoreStateMachine::default();

    let cases = crate::meta_service::raftmeta_test::cases_add_file();

    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm.apply(5, &ClientRequest {
            txid: txid.clone(),
            cmd: Cmd::AddFile {
                key: k.to_string(),
                value: v.to_string(),
            },
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
async fn test_state_machine_apply_set_file() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut sm = MemStoreStateMachine::default();

    let cases = crate::meta_service::raftmeta_test::cases_set_file();

    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm.apply(5, &ClientRequest {
            txid: txid.clone(),
            cmd: Cmd::SetFile {
                key: k.to_string(),
                value: v.to_string(),
            },
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
async fn test_state_machine_apply_incr_seq() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut sm = MemStoreStateMachine::default();

    let cases = crate::meta_service::raftmeta_test::cases_incr_seq();

    for (name, txid, k, want) in cases.iter() {
        let resp = sm.apply(5, &ClientRequest {
            txid: txid.clone(),
            cmd: Cmd::IncrSeq { key: k.to_string() },
        });
        assert_eq!(
            ClientResponse::Seq { seq: *want },
            resp.unwrap(),
            "{}",
            name
        );
    }

    Ok(())
}
