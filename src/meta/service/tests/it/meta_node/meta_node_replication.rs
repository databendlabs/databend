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

use databend_common_arrow::arrow::array::ViewType;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_sled_store::openraft::error::SnapshotMismatch;
use databend_common_meta_sled_store::openraft::testing::log_id;
use databend_common_meta_sled_store::openraft::LogIdOptionExt;
use databend_common_meta_sled_store::openraft::ServerState;
use databend_common_meta_types::protobuf::SnapshotChunkRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::InstallSnapshotError;
use databend_common_meta_types::InstallSnapshotRequest;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::RaftError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::SnapshotResponse;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::Vote;
use databend_meta::meta_service::MetaNode;
use futures::stream;
use log::info;
use maplit::btreeset;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::meta_node::start_meta_node_cluster;
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
        .applied_index(Some(log_index), "leader init logs")
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
        .applied_index(Some(log_index), "applied on leader")
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
        .applied_index(Some(log_index), "non-voter replicated all logs")
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

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_raft_service_snapshot_id_mismatch() -> anyhow::Result<()> {
    // Test SnapshotIdMismatch error should be responded.

    let (mut _nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let tc0 = tcs.remove(0);

    let mut client0 = tc0.raft_client().await?;
    let mut r1 = InstallSnapshotRequest {
        vote: Vote::new_committed(10, 2),
        meta: SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: MetaSnapshotId::new(None, 1).to_string(),
        },
        offset: 0,
        data: vec![1, 2, 3],
        done: false,
    };

    let req = SnapshotChunkRequest::new_v1(r1.clone());
    client0.install_snapshot_v1(req).await?;

    r1.meta.snapshot_id = MetaSnapshotId::new(None, 2).to_string();
    r1.offset = 3;
    let req = SnapshotChunkRequest::new_v1(r1);
    let resp = client0.install_snapshot_v1(req).await?;

    let reply = resp.into_inner();

    let err: RaftError<InstallSnapshotError> = serde_json::from_str(&reply.error)?;

    assert_eq!(
        err.api_error().unwrap(),
        &InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
            expect: (MetaSnapshotId::new(None, 2), 0).into(),
            got: (MetaSnapshotId::new(None, 2), 3).into(),
        })
    );

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_raft_service_install_snapshot_v003() -> anyhow::Result<()> {
    // Transmit snapshot in one-piece in a stream via API install_snapshot_v003.

    let (_nlog, mut tcs) = start_meta_node_cluster(btreeset![0], btreeset![]).await?;
    let tc0 = tcs.remove(0);

    let mut client0 = tc0.raft_client().await?;

    let last_log_id = log_id(10, 2, 4);

    let snapshot_meta = SnapshotMeta {
        last_log_id: Some(last_log_id),
        last_membership: StoredMembership::default(),
        snapshot_id: MetaSnapshotId::new(Some(last_log_id), 1).to_string(),
    };

    let data = [
        r#"{"DataHeader":{"key":"header","value":{"version":"V002","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":10,"node_id":2},"index":4}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}}}}}"#,
    ];

    // Complete transmit

    let strm_data = [
        SnapshotChunkRequestV003::new_chunk(data[0].to_bytes().to_vec()),
        SnapshotChunkRequestV003::new_chunk("\n".as_bytes().to_vec()),
        SnapshotChunkRequestV003::new_chunk(data[1].to_bytes().to_vec()),
        SnapshotChunkRequestV003::new_chunk("\n".as_bytes().to_vec()),
        SnapshotChunkRequestV003::new_chunk(data[2].to_bytes().to_vec()),
        SnapshotChunkRequestV003::new_end_chunk(Vote::new_committed(10, 2), snapshot_meta),
    ];

    let resp = client0
        .install_snapshot_v003(stream::iter(strm_data))
        .await?;
    let reply = resp.into_inner();

    let resp: SnapshotResponse = serde_json::from_str(&reply.data)?;

    assert_eq!(resp.vote, Vote::new_committed(10, 2));

    let meta_node = tc0.meta_node.as_ref().unwrap();
    let m = meta_node.raft.metrics().borrow().clone();

    assert_eq!(Some(last_log_id), m.snapshot);

    // Incomplete

    let strm_data = [
        SnapshotChunkRequestV003::new_chunk(data[0].to_bytes().to_vec()),
        SnapshotChunkRequestV003::new_chunk("\n".as_bytes().to_vec()),
    ];

    let err = client0
        .install_snapshot_v003(stream::iter(strm_data))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    Ok(())
}
