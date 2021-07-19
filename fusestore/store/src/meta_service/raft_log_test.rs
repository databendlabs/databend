// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::raft::Entry;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::LogId;
use common_runtime::tokio;

use crate::meta_service::raft_log::RaftLog;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::tests::service::new_sled_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_log_open() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    RaftLog::open(db).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_log_append_and_range_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db).await?;

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "foo".to_string(),
                    },
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 10 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId {
                term: 1,
                index: 256,
            },
            payload: EntryPayload::Blank,
        },
    ];

    rl.append(&logs).await?;

    let got = rl.range_get(0..)?;
    assert_eq!(logs, got);

    let got = rl.range_get(0..=2)?;
    assert_eq!(logs[0..1], got);

    let got = rl.range_get(0..3)?;
    assert_eq!(logs[0..1], got);

    let got = rl.range_get(0..5)?;
    assert_eq!(logs[0..2], got);

    let got = rl.range_get(0..10)?;
    assert_eq!(logs[0..3], got);

    let got = rl.range_get(0..11)?;
    assert_eq!(logs[0..4], got);

    let got = rl.range_get(9..11)?;
    assert_eq!(logs[2..4], got);

    let got = rl.range_get(10..256)?;
    assert_eq!(logs[3..4], got);

    let got = rl.range_get(10..257)?;
    assert_eq!(logs[3..5], got);

    let got = rl.range_get(257..)?;
    assert_eq!(logs[5..], got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_log_insert() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db).await?;

    assert_eq!(None, rl.get(&5)?);

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "foo".to_string(),
                    },
                },
            }),
        },
    ];

    for log in logs.iter() {
        rl.insert(log).await?;
    }

    assert_eq!(logs, rl.range_get(..)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_log_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db).await?;

    assert_eq!(None, rl.get(&5)?);

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "foo".to_string(),
                    },
                },
            }),
        },
    ];

    rl.append(&logs).await?;

    assert_eq!(None, rl.get(&1)?);
    assert_eq!(Some(logs[0].clone()), rl.get(&2)?);
    assert_eq!(None, rl.get(&3)?);
    assert_eq!(Some(logs[1].clone()), rl.get(&4)?);
    assert_eq!(None, rl.get(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_log_last() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db).await?;

    assert_eq!(None, rl.last()?);

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "foo".to_string(),
                    },
                },
            }),
        },
    ];

    rl.append(&logs).await?;
    assert_eq!(Some((4, logs[1].clone())), rl.last()?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_log_range_delete() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db).await?;

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "foo".to_string(),
                    },
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 10 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId {
                term: 1,
                index: 256,
            },
            payload: EntryPayload::Blank,
        },
    ];

    rl.append(&logs).await?;
    rl.range_delete(0..).await?;
    assert_eq!(logs[5..], rl.range_get(0..)?);

    rl.append(&logs).await?;
    rl.range_delete(1..).await?;
    assert_eq!(logs[5..], rl.range_get(0..)?);

    rl.append(&logs).await?;
    rl.range_delete(3..).await?;
    assert_eq!(logs[0..1], rl.range_get(0..)?);

    rl.append(&logs).await?;
    rl.range_delete(3..10).await?;
    assert_eq!(logs[0..1], rl.range_get(0..5)?);
    assert_eq!(logs[3..], rl.range_get(5..)?);

    Ok(())
}
