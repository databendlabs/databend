// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use common_runtime::tokio;

use crate::configs;
use crate::meta_service::raft_state::RaftState;
use crate::meta_service::NodeId;
use crate::tests::service::new_sled_test_context;

fn conf_of_id(id: NodeId) -> configs::Config {
    let mut c = configs::Config::empty();
    c.id = id;
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_create() -> anyhow::Result<()> {
    // - create a raft state
    // - creating another raft state in the same sled db should fail

    let tc = new_sled_test_context();
    let db = &tc.db;
    let (rs, is_open) = RaftState::open_create(db, None, Some(&conf_of_id(3))).await?;

    assert_eq!(3, rs.id);
    assert!(!is_open);

    let res = RaftState::open_create(db, None, Some(&conf_of_id(4))).await;
    assert!(res.is_err());
    assert_eq!(
        "Code: 2402, displayText = raft state present id=3, can not create.",
        res.unwrap_err().to_string()
    );

    let res = RaftState::open_create(db, None, Some(&conf_of_id(3))).await;
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

    let tc = new_sled_test_context();
    let db = &tc.db;
    let (rs, is_open) = RaftState::open_create(db, None, Some(&conf_of_id(3))).await?;

    assert_eq!(3, rs.id);
    assert!(!is_open);

    let (rs, is_open) = RaftState::open_create(db, Some(()), None).await?;
    assert_eq!(3, rs.id);
    assert!(is_open);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_open_or_create() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let (rs, is_open) = RaftState::open_create(db, Some(()), Some(&conf_of_id(3))).await?;

    assert_eq!(3, rs.id);
    assert!(!is_open);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_hard_state() -> anyhow::Result<()> {
    // - create a raft state
    // - write hard_state and the read it.

    let tc = new_sled_test_context();
    let db = &tc.db;
    let (rs, _) = RaftState::open_create(db, None, Some(&conf_of_id(3))).await?;

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
