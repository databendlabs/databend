// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::raft::Entry;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::LogId;
use common_runtime::tokio;

use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::LogIndex;
use crate::meta_service::SledTree;
use crate::tests::service::new_sled_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_open() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    SledTree::<LogIndex, Entry<LogEntry>>::open(db, "foo").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_append() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

    let logs: Vec<(LogIndex, Entry<LogEntry>)> = vec![
        (8, Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        }),
        (5, Entry {
            log_id: LogId { term: 3, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "foo".to_string(),
                    },
                },
            }),
        }),
    ];

    rl.append(&logs).await?;

    let want: Vec<Entry<LogEntry>> = vec![
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
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
    ];

    let got = rl.range_get(0..)?;
    assert_eq!(want, got);

    let got = rl.range_get(0..=5)?;
    assert_eq!(want[0..1], got);

    let got = rl.range_get(6..9)?;
    assert_eq!(want[1..], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_append_values_and_range_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

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

    rl.append_values(&logs).await?;

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
async fn test_sled_tree_range_keys() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 10 },
            payload: EntryPayload::Blank,
        },
    ];

    rl.append_values(&logs).await?;

    let got = rl.range_keys(0..)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = rl.range_keys(0..=2)?;
    assert_eq!(vec![2], got);

    let got = rl.range_keys(0..3)?;
    assert_eq!(vec![2], got);

    let got = rl.range_keys(0..10)?;
    assert_eq!(vec![2, 9], got);

    let got = rl.range_keys(0..11)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = rl.range_keys(9..11)?;
    assert_eq!(vec![9, 10], got);

    let got = rl.range_keys(10..256)?;
    assert_eq!(vec![10], got);

    let got = rl.range_keys(11..)?;
    assert_eq!(Vec::<LogIndex>::new(), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_insert() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

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
        rl.insert_value(log).await?;
    }

    assert_eq!(logs, rl.range_get(..)?);

    // insert and override

    let override_2 = Entry {
        log_id: LogId { term: 10, index: 2 },
        payload: EntryPayload::Blank,
    };

    let prev = rl.insert_value(&override_2).await?;
    assert_eq!(Some(logs[0].clone()), prev);

    // insert and override nothing

    let override_nothing = Entry {
        log_id: LogId {
            term: 10,
            index: 100,
        },
        payload: EntryPayload::Blank,
    };

    let prev = rl.insert_value(&override_nothing).await?;
    assert_eq!(None, prev);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_contains_key() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

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

    rl.append_values(&logs).await?;

    assert!(!rl.contains_key(&1)?);
    assert!(rl.contains_key(&2)?);
    assert!(!rl.contains_key(&3)?);
    assert!(rl.contains_key(&4)?);
    assert!(!rl.contains_key(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

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

    rl.append_values(&logs).await?;

    assert_eq!(None, rl.get(&1)?);
    assert_eq!(Some(logs[0].clone()), rl.get(&2)?);
    assert_eq!(None, rl.get(&3)?);
    assert_eq!(Some(logs[1].clone()), rl.get(&4)?);
    assert_eq!(None, rl.get(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_last() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

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

    rl.append_values(&logs).await?;
    assert_eq!(Some((4, logs[1].clone())), rl.last()?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_tree_range_delete() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let rl = SledTree::<LogIndex, Entry<LogEntry>>::open(db, "log").await?;

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

    rl.append_values(&logs).await?;
    rl.range_delete(0.., false).await?;
    assert_eq!(logs[5..], rl.range_get(0..)?);

    rl.append_values(&logs).await?;
    rl.range_delete(1.., false).await?;
    assert_eq!(logs[5..], rl.range_get(0..)?);

    rl.append_values(&logs).await?;
    rl.range_delete(3.., true).await?;
    assert_eq!(logs[0..1], rl.range_get(0..)?);

    rl.append_values(&logs).await?;
    rl.range_delete(3..10, true).await?;
    assert_eq!(logs[0..1], rl.range_get(0..5)?);
    assert_eq!(logs[3..], rl.range_get(5..)?);

    Ok(())
}
