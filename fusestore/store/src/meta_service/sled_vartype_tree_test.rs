// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::raft::Entry;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::LogId;
use common_runtime::tokio;

use crate::meta_service::sledkv;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::LogIndex;
use crate::meta_service::SledVarTypeTree;
use crate::meta_service::StateMachineMeta;
use crate::meta_service::StateMachineMetaKey::Initialized;
use crate::meta_service::StateMachineMetaKey::LastApplied;
use crate::meta_service::StateMachineMetaValue;
use crate::tests::service::new_sled_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_open() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    SledVarTypeTree::open(db, "foo", true).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_append() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

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

    tree.append::<sledkv::Logs>(&logs).await?;

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

    let got = tree.range_get::<sledkv::Logs, _>(0..)?;
    assert_eq!(want, got);

    let got = tree.range_get::<sledkv::Logs, _>(0..=5)?;
    assert_eq!(want[0..1], got);

    let got = tree.range_get::<sledkv::Logs, _>(6..9)?;
    assert_eq!(want[1..], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_append_values_and_range_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

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

    tree.append_values::<sledkv::Logs>(&logs).await?;

    let got = tree.range_get::<sledkv::Logs, _>(0..)?;
    assert_eq!(logs, got);

    let got = tree.range_get::<sledkv::Logs, _>(0..=2)?;
    assert_eq!(logs[0..1], got);

    let got = tree.range_get::<sledkv::Logs, _>(0..3)?;
    assert_eq!(logs[0..1], got);

    let got = tree.range_get::<sledkv::Logs, _>(0..5)?;
    assert_eq!(logs[0..2], got);

    let got = tree.range_get::<sledkv::Logs, _>(0..10)?;
    assert_eq!(logs[0..3], got);

    let got = tree.range_get::<sledkv::Logs, _>(0..11)?;
    assert_eq!(logs[0..4], got);

    let got = tree.range_get::<sledkv::Logs, _>(9..11)?;
    assert_eq!(logs[2..4], got);

    let got = tree.range_get::<sledkv::Logs, _>(10..256)?;
    assert_eq!(logs[3..4], got);

    let got = tree.range_get::<sledkv::Logs, _>(10..257)?;
    assert_eq!(logs[3..5], got);

    let got = tree.range_get::<sledkv::Logs, _>(257..)?;
    assert_eq!(logs[5..], got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_range_keys() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

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

    tree.append_values::<sledkv::Logs>(&logs).await?;

    let got = tree.range_keys::<sledkv::Logs, _>(0..)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = tree.range_keys::<sledkv::Logs, _>(0..=2)?;
    assert_eq!(vec![2], got);

    let got = tree.range_keys::<sledkv::Logs, _>(0..3)?;
    assert_eq!(vec![2], got);

    let got = tree.range_keys::<sledkv::Logs, _>(0..10)?;
    assert_eq!(vec![2, 9], got);

    let got = tree.range_keys::<sledkv::Logs, _>(0..11)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = tree.range_keys::<sledkv::Logs, _>(9..11)?;
    assert_eq!(vec![9, 10], got);

    let got = tree.range_keys::<sledkv::Logs, _>(10..256)?;
    assert_eq!(vec![10], got);

    let got = tree.range_keys::<sledkv::Logs, _>(11..)?;
    assert_eq!(Vec::<LogIndex>::new(), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_insert() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

    assert!(tree.get::<sledkv::Logs>(&5)?.is_none());

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
        tree.insert_value::<sledkv::Logs>(log).await?;
    }

    assert_eq!(logs, tree.range_get::<sledkv::Logs, _>(..)?);

    // insert and override

    let override_2 = Entry {
        log_id: LogId { term: 10, index: 2 },
        payload: EntryPayload::Blank,
    };

    let prev = tree.insert_value::<sledkv::Logs>(&override_2).await?;
    assert_eq!(Some(logs[0].clone()), prev);

    // insert and override nothing

    let override_nothing = Entry {
        log_id: LogId {
            term: 10,
            index: 100,
        },
        payload: EntryPayload::Blank,
    };

    let prev = tree.insert_value::<sledkv::Logs>(&override_nothing).await?;
    assert_eq!(None, prev);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_contains_key() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

    assert!(tree.get::<sledkv::Logs>(&5)?.is_none());

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

    tree.append_values::<sledkv::Logs>(&logs).await?;

    assert!(!tree.contains_key::<sledkv::Logs>(&1)?);
    assert!(tree.contains_key::<sledkv::Logs>(&2)?);
    assert!(!tree.contains_key::<sledkv::Logs>(&3)?);
    assert!(tree.contains_key::<sledkv::Logs>(&4)?);
    assert!(!tree.contains_key::<sledkv::Logs>(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

    assert!(tree.get::<sledkv::Logs>(&5)?.is_none());

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

    tree.append_values::<sledkv::Logs>(&logs).await?;

    assert_eq!(None, tree.get::<sledkv::Logs>(&1)?);
    assert_eq!(Some(logs[0].clone()), tree.get::<sledkv::Logs>(&2)?);
    assert_eq!(None, tree.get::<sledkv::Logs>(&3)?);
    assert_eq!(Some(logs[1].clone()), tree.get::<sledkv::Logs>(&4)?);
    assert_eq!(None, tree.get::<sledkv::Logs>(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_last() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

    assert!(tree.last::<sledkv::Logs>()?.is_none());

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

    tree.append_values::<sledkv::Logs>(&logs).await?;
    assert_eq!(None, tree.last::<StateMachineMeta>()?);

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(LogId { term: 1, index: 2 }),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];

    tree.append::<StateMachineMeta>(metas.as_slice()).await?;

    assert_eq!(Some((4, logs[1].clone())), tree.last::<sledkv::Logs>()?);
    assert_eq!(
        Some((Initialized, StateMachineMetaValue::Bool(true))),
        tree.last::<StateMachineMeta>()?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_range_delete() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

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

    tree.append_values::<sledkv::Logs>(&logs).await?;
    tree.range_delete::<sledkv::Logs, _>(0.., false).await?;
    assert_eq!(logs[5..], tree.range_get::<sledkv::Logs, _>(0..)?);

    tree.append_values::<sledkv::Logs>(&logs).await?;
    tree.range_delete::<sledkv::Logs, _>(1.., false).await?;
    assert_eq!(logs[5..], tree.range_get::<sledkv::Logs, _>(0..)?);

    tree.append_values::<sledkv::Logs>(&logs).await?;
    tree.range_delete::<sledkv::Logs, _>(3.., true).await?;
    assert_eq!(logs[0..1], tree.range_get::<sledkv::Logs, _>(0..)?);

    tree.append_values::<sledkv::Logs>(&logs).await?;
    tree.range_delete::<sledkv::Logs, _>(3..10, true).await?;
    assert_eq!(logs[0..1], tree.range_get::<sledkv::Logs, _>(0..5)?);
    assert_eq!(logs[3..], tree.range_get::<sledkv::Logs, _>(5..)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_vartype_tree_multi_types() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;

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

    tree.append_values::<sledkv::Logs>(&logs).await?;

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(LogId { term: 1, index: 2 }),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];
    tree.append::<StateMachineMeta>(&metas).await?;

    // range get/keys are limited to its own namespace.
    {
        let got = tree.range_get::<sledkv::Logs, _>(..)?;
        assert_eq!(logs, got);

        let got = tree.range_get::<StateMachineMeta, _>(..=LastApplied)?;
        assert_eq!(
            vec![StateMachineMetaValue::LogId(LogId { term: 1, index: 2 })],
            got
        );

        let got = tree.range_get::<StateMachineMeta, _>(Initialized..)?;
        assert_eq!(vec![StateMachineMetaValue::Bool(true)], got);

        let got = tree.range_keys::<StateMachineMeta, _>(Initialized..)?;
        assert_eq!(vec![Initialized], got);
    }

    // range delete are limited to its own namespace.
    {
        tree.range_delete::<StateMachineMeta, _>(.., false).await?;

        let got = tree.range_get::<sledkv::Logs, _>(..)?;
        assert_eq!(logs, got);
    }

    Ok(())
}

// --- AsType test ---

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_append() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

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

    aslog.append(&logs).await?;

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

    let got = aslog.range_get(0..)?;
    assert_eq!(want, got);

    let got = aslog.range_get(0..=5)?;
    assert_eq!(want[0..1], got);

    let got = aslog.range_get(6..9)?;
    assert_eq!(want[1..], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_append_values_and_range_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

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

    aslog.append_values(&logs).await?;

    let got = aslog.range_get(0..)?;
    assert_eq!(logs, got);

    let got = aslog.range_get(0..=2)?;
    assert_eq!(logs[0..1], got);

    let got = aslog.range_get(0..3)?;
    assert_eq!(logs[0..1], got);

    let got = aslog.range_get(0..5)?;
    assert_eq!(logs[0..2], got);

    let got = aslog.range_get(0..10)?;
    assert_eq!(logs[0..3], got);

    let got = aslog.range_get(0..11)?;
    assert_eq!(logs[0..4], got);

    let got = aslog.range_get(9..11)?;
    assert_eq!(logs[2..4], got);

    let got = aslog.range_get(10..256)?;
    assert_eq!(logs[3..4], got);

    let got = aslog.range_get(10..257)?;
    assert_eq!(logs[3..5], got);

    let got = aslog.range_get(257..)?;
    assert_eq!(logs[5..], got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_range_keys() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

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

    aslog.append_values(&logs).await?;

    let got = aslog.range_keys(0..)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = aslog.range_keys(0..=2)?;
    assert_eq!(vec![2], got);

    let got = aslog.range_keys(0..3)?;
    assert_eq!(vec![2], got);

    let got = aslog.range_keys(0..10)?;
    assert_eq!(vec![2, 9], got);

    let got = aslog.range_keys(0..11)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = aslog.range_keys(9..11)?;
    assert_eq!(vec![9, 10], got);

    let got = aslog.range_keys(10..256)?;
    assert_eq!(vec![10], got);

    let got = aslog.range_keys(11..)?;
    assert_eq!(Vec::<LogIndex>::new(), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_insert() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

    assert_eq!(None, aslog.get(&5)?);

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
        aslog.insert_value(log).await?;
    }

    assert_eq!(logs, aslog.range_get(..)?);

    // insert and override

    let override_2 = Entry {
        log_id: LogId { term: 10, index: 2 },
        payload: EntryPayload::Blank,
    };

    let prev = aslog.insert_value(&override_2).await?;
    assert_eq!(Some(logs[0].clone()), prev);

    // insert and override nothing

    let override_nothing = Entry {
        log_id: LogId {
            term: 10,
            index: 100,
        },
        payload: EntryPayload::Blank,
    };

    let prev = aslog.insert_value(&override_nothing).await?;
    assert_eq!(None, prev);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_contains_key() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

    assert_eq!(None, aslog.get(&5)?);

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

    aslog.append_values(&logs).await?;

    assert!(!aslog.contains_key(&1)?);
    assert!(aslog.contains_key(&2)?);
    assert!(!aslog.contains_key(&3)?);
    assert!(aslog.contains_key(&4)?);
    assert!(!aslog.contains_key(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

    assert_eq!(None, aslog.get(&5)?);

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

    aslog.append_values(&logs).await?;

    assert_eq!(None, aslog.get(&1)?);
    assert_eq!(Some(logs[0].clone()), aslog.get(&2)?);
    assert_eq!(None, aslog.get(&3)?);
    assert_eq!(Some(logs[1].clone()), aslog.get(&4)?);
    assert_eq!(None, aslog.get(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_last() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

    assert_eq!(None, aslog.last()?);

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

    aslog.append_values(&logs).await?;
    assert_eq!(Some((4, logs[1].clone())), aslog.last()?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_range_delete() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();

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

    aslog.append_values(&logs).await?;
    aslog.range_delete(0.., false).await?;
    assert_eq!(logs[5..], aslog.range_get(0..)?);

    aslog.append_values(&logs).await?;
    aslog.range_delete(1.., false).await?;
    assert_eq!(logs[5..], aslog.range_get(0..)?);

    aslog.append_values(&logs).await?;
    aslog.range_delete(3.., true).await?;
    assert_eq!(logs[0..1], aslog.range_get(0..)?);

    aslog.append_values(&logs).await?;
    aslog.range_delete(3..10, true).await?;
    assert_eq!(logs[0..1], aslog.range_get(0..5)?);
    assert_eq!(logs[3..], aslog.range_get(5..)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_multi_types() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledVarTypeTree::open(db, "foo", true).await?;
    let aslog = tree.as_type::<sledkv::Logs>();
    let asmeta = tree.as_type::<StateMachineMeta>();

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

    aslog.append_values(&logs).await?;

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(LogId { term: 1, index: 2 }),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];
    asmeta.append(&metas).await?;

    // range get/keys are limited to its own namespace.
    {
        let got = aslog.range_get(..)?;
        assert_eq!(logs, got);

        let got = asmeta.range_get(..=LastApplied)?;
        assert_eq!(
            vec![StateMachineMetaValue::LogId(LogId { term: 1, index: 2 })],
            got
        );

        let got = asmeta.range_get(Initialized..)?;
        assert_eq!(vec![StateMachineMetaValue::Bool(true)], got);

        let got = asmeta.range_keys(Initialized..)?;
        assert_eq!(vec![Initialized], got);
    }

    // range delete are limited to its own namespace.
    {
        asmeta.range_delete(.., false).await?;

        let got = aslog.range_get(..)?;
        assert_eq!(logs, got);
    }

    Ok(())
}
