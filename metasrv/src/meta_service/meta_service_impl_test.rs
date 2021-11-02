// Copyright 2020 Datafuse Labs.
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

use async_raft::State;
use common_base::tokio;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
#[allow(unused_imports)]
use log::info;
use pretty_assertions::assert_eq;

use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::RetryableError;
use crate::tests::assert_meta_connection;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_upsert_kv() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let addr = tc.config.raft_config.raft_api_addr();

    let _mn = MetaNode::boot(0, &tc.config.raft_config).await?;
    assert_meta_connection(&addr).await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    {
        // add: ok
        let req = LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: "foo".to_string(),
                seq: MatchSeq::Exact(0),
                value: Operation::Update(b"bar".to_vec()),
                value_meta: None,
            },
        };
        let raft_mes = client.write(req).await?.into_inner();

        let rst: Result<AppliedState, RetryableError> = raft_mes.into();
        let resp: AppliedState = rst?;
        match resp {
            AppliedState::KV(Change { prev, result }) => {
                assert!(prev.is_none());
                let sv = result.unwrap();
                assert!(sv.seq > 0);
                assert!(sv.meta.is_none());
                assert_eq!(b"bar".to_vec(), sv.data);
            }
            _ => {
                panic!("not KV")
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_incr_seq() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let addr = tc.config.raft_config.raft_api_addr();

    let _mn = MetaNode::boot(0, &tc.config.raft_config).await?;
    assert_meta_connection(&addr).await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    let cases = common_meta_raft_store::state_machine::testing::cases_incr_seq();

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

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc0 = new_test_context();
    let tc1 = new_test_context();

    let addr0 = tc0.config.raft_config.raft_api_addr();
    let addr1 = tc1.config.raft_config.raft_api_addr();

    let mn0 = MetaNode::boot(0, &tc0.config.raft_config).await?;
    assert_meta_connection(&addr0).await?;

    {
        // add node 1 as non-voter
        let mn1 = MetaNode::boot_non_voter(1, &tc1.config.raft_config).await?;
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
        cmd: Cmd::UpsertKV {
            key: "t-write-on-non-voter".to_string(),
            seq: MatchSeq::Any,
            value: Operation::Update(b"1".to_vec()),
            value_meta: None,
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

// TODO revert meta tests from git history
