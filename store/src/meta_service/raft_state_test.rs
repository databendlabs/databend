// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use async_raft::raft::MembershipConfig;
use async_raft::storage::HardState;
use async_raft::LogId;
use async_raft::SnapshotMeta;
use common_runtime::tokio;
use maplit::hashset;

use crate::meta_service::raft_state::RaftState;
use crate::tests::service::init_store_unittest;
use crate::tests::service::new_sled_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_create() -> anyhow::Result<()> {
    // - create a raft state
    // - creating another raft state in the same sled db should fail

    init_store_unittest();

    let mut tc = new_sled_test_context();
    let db = &tc.db;
    tc.config.id = 3;
    let rs = RaftState::open_create(db, &tc.config, None, Some(())).await?;
    let is_open = rs.is_open();

    assert_eq!(3, rs.id);
    assert!(!is_open);

    tc.config.id = 4;
    let res = RaftState::open_create(db, &tc.config, None, Some(())).await;
    assert!(res.is_err());
    assert_eq!(
        "Code: 2402, displayText = raft state present id=3, can not create.",
        res.unwrap_err().to_string()
    );

    tc.config.id = 3;
    let res = RaftState::open_create(db, &tc.config, None, Some(())).await;
    assert!(res.is_err());
    assert_eq!(
        "Code: 2402, displayText = raft state present id=3, can not create.",
        res.unwrap_err().to_string()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_open() -> anyhow::Result<()> {
    // - create a raft state
    // - open it.

    init_store_unittest();

    let mut tc = new_sled_test_context();
    let db = &tc.db;
    tc.config.id = 3;
    let rs = RaftState::open_create(db, &tc.config, None, Some(())).await?;
    let is_open = rs.is_open();

    assert_eq!(3, rs.id);
    assert!(!is_open);

    tc.config.id = 1000;
    let rs = RaftState::open_create(db, &tc.config, Some(()), None).await?;
    let is_open = rs.is_open();
    assert_eq!(3, rs.id);
    assert!(is_open);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_open_or_create() -> anyhow::Result<()> {
    init_store_unittest();

    let mut tc = new_sled_test_context();
    let db = &tc.db;
    tc.config.id = 3;
    let rs = RaftState::open_create(db, &tc.config, Some(()), Some(())).await?;
    let is_open = rs.is_open();

    assert_eq!(3, rs.id);
    assert!(!is_open);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_hard_state() -> anyhow::Result<()> {
    // - create a raft state
    // - write hard_state and the read it.
    init_store_unittest();

    let mut tc = new_sled_test_context();
    let db = &tc.db;
    tc.config.id = 3;
    let rs = RaftState::open_create(db, &tc.config, None, Some(())).await?;

    assert_eq!(3, rs.id);

    // read got a None

    let got = rs.read_hard_state()?;
    assert_eq!(None, got);

    // write hard state

    let hs = HardState {
        current_term: 10,
        voted_for: Some(3),
    };

    rs.write_hard_state(&hs).await?;

    // read the written

    let got = rs.read_hard_state()?;
    assert_eq!(Some(hs), got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_state_machine_id() -> anyhow::Result<()> {
    // - create a raft state
    // - write state machine id and the read it.
    init_store_unittest();

    let mut tc = new_sled_test_context();
    let db = &tc.db;
    tc.config.id = 3;
    let rs = RaftState::open_create(db, &tc.config, None, Some(())).await?;

    // read got a None

    let got = rs.read_state_machine_id()?;
    assert_eq!((0, 0), got);

    // write hard state

    let smid = (1, 2);

    rs.write_state_machine_id(&smid).await?;

    // read the written

    let got = rs.read_state_machine_id()?;
    assert_eq!((1, 2), got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_removed_snapshot() -> anyhow::Result<()> {
    // - create a raft state
    // - write state membership and the read it.
    init_store_unittest();

    let mut tc = new_sled_test_context();
    let db = &tc.db;
    tc.config.id = 3;
    let rs = RaftState::open_create(db, &tc.config, None, Some(())).await?;

    // read got a None

    let default = SnapshotMeta {
        last_log_id: LogId { term: 0, index: 0 },
        membership: MembershipConfig {
            members: hashset![tc.config.id],
            members_after_consensus: None,
        },
        snapshot_id: "".to_string(),
    };

    let got = rs.read_snapshot_meta()?;
    assert_eq!(default.last_log_id, got.last_log_id);
    assert_eq!(default.membership, got.membership);
    assert_eq!(default.snapshot_id, got.snapshot_id);

    // write hard state

    let snap_meta = SnapshotMeta {
        last_log_id: LogId { term: 1, index: 2 },
        membership: MembershipConfig {
            members: hashset![1, 2, 3],
            members_after_consensus: None,
        },
        snapshot_id: "".to_string(),
    };

    rs.write_snapshot_meta(&snap_meta).await?;

    // read the written

    let got = rs.read_snapshot_meta()?;
    assert_eq!(snap_meta.last_log_id, got.last_log_id);
    assert_eq!(snap_meta.membership, got.membership);
    assert_eq!(snap_meta.snapshot_id, got.snapshot_id);
    Ok(())
}
