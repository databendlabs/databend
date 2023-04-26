// Copyright 2023 Datafuse Labs.
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

//! Test raft protocol behaviors

use std::time::Duration;

use common_base::base::tokio;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::UpsertKV;
use databend_meta::init_meta_ut;
use maplit::btreeset;
use tracing::info;

use crate::tests::meta_node::start_meta_node_cluster;

/// When a follower is dumping a snapshot, it should not block append entries request.
/// Thus heartbeat should still be processed, and logs can be committed by leader(but not by followers).
///
/// Building a snapshot includes two steps:
/// 1. Dumping the state machine to a in-memory struct.
/// 2. Serialize the dumped data.
#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_dumping_snapshot_does_not_block_append_entries() -> anyhow::Result<()> {
    info!("--- initialize cluster 2 voters");
    let (mut _log_index, mut tcs) = start_meta_node_cluster(btreeset![0, 1], btreeset![]).await?;

    let tc0 = tcs.remove(0);
    let tc1 = tcs.remove(0);

    let mn0 = tc0.meta_node.clone().unwrap();
    let mn1 = tc1.meta_node.clone().unwrap();

    info!("--- block dumping snapshot from state machine for 5 seconds");
    {
        let mut sm = mn1.sto.get_state_machine().await;
        let mut blocking_config = sm.blocking_config_mut();
        blocking_config.dump_snapshot = Duration::from_secs(5);
    }

    info!("--- trigger building snapshot");
    mn1.raft.trigger_snapshot().await?;

    info!("--- Wait 500 ms for snapshot to be begin building");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    info!("--- With snapshot being blocked, leader can still write");
    let key = "foo";
    mn0.assume_leader()
        .await?
        .write(LogEntry {
            txid: None,
            time_ms: None,
            cmd: Cmd::UpsertKV(UpsertKV::update(key, key.as_bytes())),
        })
        .await?;
    info!("--- Write done");

    Ok(())
}

/// When a follower is serializing a snapshot, it should not block append entries request.
/// Thus heartbeat should still be processed, and logs can be committed by leader(but not by followers).
///
/// Building a snapshot includes two steps:
/// 1. Dumping the state machine to a in-memory struct.
/// 2. Serialize the dumped data.
#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_serializing_snapshot_does_not_block_append_entries() -> anyhow::Result<()> {
    info!("--- initialize cluster 2 voters");
    let (mut _log_index, mut tcs) = start_meta_node_cluster(btreeset![0, 1], btreeset![]).await?;

    let tc0 = tcs.remove(0);
    let tc1 = tcs.remove(0);

    let mn0 = tc0.meta_node.clone().unwrap();
    let mn1 = tc1.meta_node.clone().unwrap();

    info!("--- block dumping snapshot from state machine for 5 seconds");
    {
        let mut sm = mn1.sto.get_state_machine().await;
        let mut blocking_config = sm.blocking_config_mut();
        blocking_config.serde_snapshot = Duration::from_secs(5);
    }

    info!("--- trigger building snapshot");
    mn1.raft.trigger_snapshot().await?;

    info!("--- Wait 500 ms for snapshot to be begin building");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    info!("--- With snapshot being blocked, leader can still write");
    let key = "foo";
    mn0.assume_leader()
        .await?
        .write(LogEntry {
            txid: None,
            time_ms: None,
            cmd: Cmd::UpsertKV(UpsertKV::update(key, key.as_bytes())),
        })
        .await?;
    info!("--- Write done");

    Ok(())
}
