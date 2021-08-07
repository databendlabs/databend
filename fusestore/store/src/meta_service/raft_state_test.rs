// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use common_runtime::tokio;

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

    let got = rs.read_hard_state().await?;
    assert_eq!(None, got);

    // write hard state

    let hs = HardState {
        current_term: 10,
        voted_for: Some(3),
    };

    rs.write_hard_state(&hs).await?;

    // read the written

    let got = rs.read_hard_state().await?;
    assert_eq!(Some(hs), got);
    Ok(())
}
