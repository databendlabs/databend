// Copyright 2021 Datafuse Labs
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

use std::time::Duration;

use databend_common_meta_kvapi::kvapi::KvApiExt;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::raft_types::new_log_id;
use databend_common_version::BUILD_INFO;
use databend_meta::meta_service::MetaNode;
use log::info;
use maplit::btreeset;
use test_harness::test;
use tokio::time::sleep;

use crate::testing::meta_service_test_harness;
use crate::testing::since_epoch_millis;
use crate::tests::meta_node::start_meta_node_cluster;
use crate::tests::meta_node::timeout;

/// Tests timestamp-based expiration consistency across snapshot boundaries.
///
/// **Scenario:**
/// - Log-1 has timestamp T+180s, included in snapshot
/// - Log-2 has timestamp T+60s with expiration T+120s, not in snapshot
///
/// **Issue:** After restart, Log-2 uses current time instead of last-seen time (from Log-1)
/// for expiration checks, potentially causing state inconsistencies between nodes.
///
/// This bug raises in real world if:
/// - When meta-service restarts, there is one log(`A`) to re-apply that has smaller `expire_at`(`t1`)
///   than the biggest timestamp(`LogEntry.time_ms`) in the snapshot,
/// - and all the log entries before `A` have smaller timestamp(`LogEntry.time_ms`) than `t1`.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_node_log_time_revert_cross_snapshot_boundary() -> anyhow::Result<()> {
    let now_ms = since_epoch_millis();

    // Log with later timestamp (T+180s) - will be included in snapshot
    let log_later = LogEntry {
        time_ms: Some(now_ms + 180_000),
        cmd: Cmd::Transaction(TxnRequest::new(vec![], vec![TxnOp::put(
            "k1",
            b"v1".to_vec(),
        )])),
    };

    // Log with earlier timestamp (T+60s) but expires at T+120s - not in snapshot
    let log_earlier = LogEntry {
        time_ms: Some(now_ms + 60_000),
        cmd: Cmd::Transaction(TxnRequest::new(vec![], vec![
            TxnOp::put("k1", b"v2".to_vec()).with_expires_at_ms(Some(now_ms + 120_000)),
        ])),
    };

    let result_with_restart = write_two_logs(log_later.clone(), log_earlier.clone(), true).await?;
    let result_without_restart = write_two_logs(log_later, log_earlier, false).await?;

    assert_eq!(
        result_with_restart.is_some(),
        result_without_restart.is_some()
    );

    Ok(())
}

/// Writes two logs with optional snapshot and restart between them.
///
/// Tests how state machine behavior differs when last-seen time context is lost
/// during restart. Returns the final value of key "k1".
async fn write_two_logs(
    first_log: LogEntry,
    second_log: LogEntry,
    restart: bool,
) -> anyhow::Result<Option<SeqV>> {
    info!("Testing log sequence with restart={}", restart);

    let (mut log_index, mut test_contexts) =
        start_meta_node_cluster(btreeset![0], btreeset![]).await?;

    let mut tc0 = test_contexts.remove(0);
    let mut meta_node = tc0.meta_node.take().unwrap();

    // Apply first log
    meta_node.raft.client_write(first_log).await?;
    log_index += 1;

    // Restart node with snapshot if requested
    if restart {
        info!("Taking snapshot at log_index={}", log_index);
        meta_node.raft.trigger().snapshot().await?;
        meta_node
            .raft
            .wait(timeout())
            .snapshot(new_log_id(1, 0, log_index), "purged")
            .await?;

        info!("Restarting meta-node");
        meta_node.stop().await?;
        drop(meta_node);

        sleep(Duration::from_secs(2)).await;

        meta_node = MetaNode::open(&tc0.config.raft_config, BUILD_INFO.semver()).await?;
    }

    // Apply second log
    meta_node.raft.client_write(second_log).await?;

    // Check final state
    let result = meta_node
        .raft_store
        .get_sm_v003()
        .kv_api()
        .get_kv("k1")
        .await?;

    info!("Final state for k1 (restart={}): {:?}", restart, result);

    Ok(result)
}
