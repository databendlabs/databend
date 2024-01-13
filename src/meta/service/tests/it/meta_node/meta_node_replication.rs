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

use databend_common_meta_sled_store::openraft::LogIdOptionExt;
use databend_common_meta_sled_store::openraft::ServerState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::UpsertKV;
use databend_meta::meta_service::MetaNode;
use log::info;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::start_meta_node_non_voter;
use crate::tests::meta_node::timeout;
use crate::tests::service::MetaSrvTestContext;

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_node_snapshot_replication() -> anyhow::Result<()> {
    // - Bring up a cluster of 3.
    // - Write just enough logs to trigger a snapshot.
    // - Add a non-voter, test the snapshot is sync-ed
    // - Write logs to trigger another snapshot.
    // - Add

    // Create a snapshot every 10 logs
    let snap_logs = 10;

    let mut tc = MetaSrvTestContext::new(0);
    tc.config.raft_config.snapshot_logs_since_last = snap_logs;
    tc.config.raft_config.install_snapshot_timeout = 10_1000; // milli seconds. In a CI multi-threads test delays async task badly.
    tc.config.raft_config.max_applied_log_to_keep = 0;

    let mn = MetaNode::boot(&tc.config).await?;

    tc.assert_raft_server_connection().await?;

    mn.raft
        .wait(timeout())
        .state(ServerState::Leader, "leader started")
        .await?;

    mn.raft
        .wait(timeout())
        .current_leader(0, "node-0 has leader")
        .await?;

    // initial membership, leader blank log, add node.
    let mut log_index = 3;

    mn.raft
        .wait(timeout())
        .log(Some(log_index), "leader init logs")
        .await?;

    let n_req = 12;

    for i in 0..n_req {
        let key = format!("test_meta_node_snapshot_replication-key-{}", i);
        mn.write(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(&key, b"v"))))
            .await?;
    }
    log_index += n_req;

    info!("--- check the log is locally applied");

    mn.raft
        .wait(timeout())
        .log(Some(log_index), "applied on leader")
        .await?;

    info!("--- check the snapshot is created");

    mn.raft
        .wait(timeout())
        .metrics(
            |x| {
                x.snapshot.map(|x| x.leader_id.term) == Some(1)
                    && x.snapshot.next_index() >= snap_logs
            },
            "snapshot is created by leader",
        )
        .await?;

    info!("--- start a non_voter to receive snapshot replication");

    let (_, tc1) = start_meta_node_non_voter(mn.clone(), 1).await?;
    // add node, change membership
    log_index += 2;

    let mn1 = tc1.meta_node();

    mn1.raft
        .wait(timeout())
        .log(Some(log_index), "non-voter replicated all logs")
        .await?;

    mn1.raft
        .wait(timeout())
        .metrics(
            |x| {
                x.snapshot.map(|x| x.leader_id.term) == Some(1)
                    && x.snapshot.next_index() >= snap_logs
            },
            "snapshot is received by non-voter",
        )
        .await?;

    for i in 0..n_req {
        let key = format!("test_meta_node_snapshot_replication-key-{}", i);
        let sm = mn1.sto.get_state_machine().await;
        let got = sm.get_maybe_expired_kv(&key).await?;
        match got {
            None => {
                panic!("expect get some value for {}", key)
            }
            Some(SeqV { ref data, .. }) => {
                assert_eq!(data, b"v");
            }
        }
    }

    Ok(())
}
