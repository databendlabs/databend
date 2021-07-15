// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use common_runtime::tokio;
use tempfile::tempdir;

use crate::meta_service::raft_state::RaftState;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_create() -> anyhow::Result<()> {
    // - create a raft state
    // - creating another raft state in the same sled db should fail

    let dir = tempdir()?;
    let root = dir.path();
    let db = sled::open(root)?;
    let rs = RaftState::create(&db, &3).await?;

    assert_eq!(3, rs.id);

    let res = RaftState::create(&db, &4).await;
    assert!(res.is_err());
    assert_eq!(
        "Code: 2402, displayText = exist: id=Ok(3).",
        res.unwrap_err().to_string()
    );

    let res = RaftState::create(&db, &3).await;
    assert!(res.is_err());
    assert_eq!(
        "Code: 2402, displayText = exist: id=Ok(3).",
        res.unwrap_err().to_string()
    );
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_open() -> anyhow::Result<()> {
    // - create a raft state
    // - open it.

    let dir = tempdir()?;
    let root = dir.path();
    let db = sled::open(root)?;
    let rs = RaftState::create(&db, &3).await?;

    assert_eq!(3, rs.id);

    let rs = RaftState::open(&db)?;
    assert_eq!(3, rs.id);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_hard_state() -> anyhow::Result<()> {
    // - create a raft state
    // - write hard_state and the read it.

    let dir = tempdir()?;
    let root = dir.path();
    let db = sled::open(root)?;
    let rs = RaftState::create(&db, &3).await?;

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
