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

use std::sync::Arc;

use databend_common_meta_sled_store::openraft::error::RaftError;
use databend_common_meta_types::ClientWriteError;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::ForwardToLeader;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::UpsertKV;
use databend_meta::meta_service::meta_leader::MetaLeader;
use databend_meta::meta_service::MetaNode;
use maplit::btreeset;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::start_meta_node_cluster;
use crate::tests::service::MetaSrvTestContext;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_node_forward_to_leader() -> anyhow::Result<()> {
    // - Start a leader, 2 followers and a non-voter;
    // - Write to the raft node on the leader, expect Ok.
    // - Write to the raft node on the non-leader, expect ForwardToLeader error.

    let (mut _nlog, tcs) = start_meta_node_cluster(btreeset![0, 1, 2], btreeset![3]).await?;
    let all = test_context_nodes(&tcs);

    let leader_id = all[0].raft.metrics().borrow().current_leader.unwrap();

    // test writing to leader and non-leader
    let key = "t-non-leader-write";
    for id in [0u64, 1, 2, 3] {
        let mn = &all[id as usize];
        let maybe_leader = MetaLeader::new(mn);
        let rst = maybe_leader
            .write(LogEntry {
                txid: None,
                time_ms: None,
                cmd: Cmd::UpsertKV(UpsertKV::update(key, key.as_bytes())),
            })
            .await;

        if id == leader_id {
            assert!(rst.is_ok());
        } else {
            assert!(rst.is_err());
            let e = rst.unwrap_err();
            match e {
                RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                    leader_id: forward_leader_id,
                    ..
                })) => {
                    assert_eq!(Some(leader_id), forward_leader_id);
                }
                _ => {
                    panic!("expect MetaRaftError::ForwardToLeader")
                }
            }
        }
    }

    Ok(())
}

fn test_context_nodes(tcs: &[MetaSrvTestContext]) -> Vec<Arc<MetaNode>> {
    tcs.iter().map(|tc| tc.meta_node()).collect::<Vec<_>>()
}
