// Copyright 2020 Datafuse Labs.
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

use async_raft::raft::Entry;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::LogId;
use common_metatypes::KVValue;
use common_runtime::tokio;

use crate::meta_service::sled_key_space;
use crate::meta_service::sled_key_space::SledKeySpace;
use crate::meta_service::sled_key_space::StateMachineMeta;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::LogIndex;
use crate::meta_service::SledTree;
use crate::meta_service::StateMachineMetaKey::Initialized;
use crate::meta_service::StateMachineMetaKey::LastApplied;
use crate::meta_service::StateMachineMetaValue;
use crate::tests::service::new_sled_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_open() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_append() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append::<sled_key_space::Logs>(&logs).await?;

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

    let got = tree.range_values::<sled_key_space::Logs, _>(0..)?;
    assert_eq!(want, got);

    let got = tree.range_values::<sled_key_space::Logs, _>(0..=5)?;
    assert_eq!(want[0..1], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(6..9)?;
    assert_eq!(want[1..], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_append_values_and_range_get() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    let got = tree.range_values::<sled_key_space::Logs, _>(0..)?;
    assert_eq!(logs, got);

    let got = tree.range_values::<sled_key_space::Logs, _>(0..=2)?;
    assert_eq!(logs[0..1], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(0..3)?;
    assert_eq!(logs[0..1], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(0..5)?;
    assert_eq!(logs[0..2], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(0..10)?;
    assert_eq!(logs[0..3], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(0..11)?;
    assert_eq!(logs[0..4], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(9..11)?;
    assert_eq!(logs[2..4], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(10..256)?;
    assert_eq!(logs[3..4], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(10..257)?;
    assert_eq!(logs[3..5], got);

    let got = tree.range_values::<sled_key_space::Logs, _>(257..)?;
    assert_eq!(logs[5..], got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_range_keys() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    let got = tree.range_keys::<sled_key_space::Logs, _>(0..)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(0..=2)?;
    assert_eq!(vec![2], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(0..3)?;
    assert_eq!(vec![2], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(0..10)?;
    assert_eq!(vec![2, 9], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(0..11)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(9..11)?;
    assert_eq!(vec![9, 10], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(10..256)?;
    assert_eq!(vec![10], got);

    let got = tree.range_keys::<sled_key_space::Logs, _>(11..)?;
    assert_eq!(Vec::<LogIndex>::new(), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_range_kvs() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    let got = tree.range_kvs::<sled_key_space::Logs, _>(9..11)?;
    assert_eq!(vec![(9, logs[1].clone()), (10, logs[2].clone())], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_range() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    // This test assumes the following order.
    // to check the range boundary.
    assert!(sled_key_space::Logs::PREFIX < StateMachineMeta::PREFIX);

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(LogId { term: 1, index: 2 }),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];

    tree.append::<StateMachineMeta>(metas.as_slice()).await?;

    let log_tree = tree.key_space::<sled_key_space::Logs>();
    let meta_tree = tree.key_space::<StateMachineMeta>();

    // key sapce Logs

    let mut it = tree.range::<sled_key_space::Logs, _>(..)?;
    assert_eq!((2, logs[0].clone()), it.next().unwrap()?);
    assert_eq!((4, logs[1].clone()), it.next().unwrap()?);
    assert!(it.next().is_none());

    let mut it = log_tree.range(..)?;
    assert_eq!((2, logs[0].clone()), it.next().unwrap()?);
    assert_eq!((4, logs[1].clone()), it.next().unwrap()?);
    assert!(it.next().is_none());

    // key sapce Logs reversed

    let mut it = tree.range::<sled_key_space::Logs, _>(..)?.rev();
    assert_eq!((4, logs[1].clone()), it.next().unwrap()?);
    assert_eq!((2, logs[0].clone()), it.next().unwrap()?);
    assert!(it.next().is_none());

    let mut it = log_tree.range(..)?.rev();
    assert_eq!((4, logs[1].clone()), it.next().unwrap()?);
    assert_eq!((2, logs[0].clone()), it.next().unwrap()?);
    assert!(it.next().is_none());

    // key space StateMachineMeta

    let mut it = tree.range::<StateMachineMeta, _>(..)?;
    assert_eq!(metas[0], it.next().unwrap()?);
    assert_eq!(metas[1], it.next().unwrap()?);
    assert!(it.next().is_none());

    let mut it = meta_tree.range(..)?;
    assert_eq!(metas[0], it.next().unwrap()?);
    assert_eq!(metas[1], it.next().unwrap()?);
    assert!(it.next().is_none());

    // key space StateMachineMeta reversed

    let mut it = tree.range::<StateMachineMeta, _>(..)?.rev();
    assert_eq!(metas[1], it.next().unwrap()?);
    assert_eq!(metas[0], it.next().unwrap()?);
    assert!(it.next().is_none());

    let mut it = meta_tree.range(..)?.rev();
    assert_eq!(metas[1], it.next().unwrap()?);
    assert_eq!(metas[0], it.next().unwrap()?);
    assert!(it.next().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_scan_prefix() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    let files: Vec<(String, String)> = vec![
        ("a".to_string(), "x".to_string()),
        ("ab".to_string(), "xy".to_string()),
        ("abc".to_string(), "xyz".to_string()),
        ("abd".to_string(), "xyZ".to_string()),
        ("b".to_string(), "y".to_string()),
    ];

    tree.append::<sled_key_space::Files>(&files).await?;

    let got = tree.scan_prefix::<sled_key_space::Files>(&"ab".to_string())?;
    assert_eq!(files[1..4], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_insert() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    assert!(tree.get::<sled_key_space::Logs>(&5)?.is_none());

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
        tree.insert_value::<sled_key_space::Logs>(log).await?;
    }

    assert_eq!(logs, tree.range_values::<sled_key_space::Logs, _>(..)?);

    // insert and override

    let override_2 = Entry {
        log_id: LogId { term: 10, index: 2 },
        payload: EntryPayload::Blank,
    };

    let prev = tree
        .insert_value::<sled_key_space::Logs>(&override_2)
        .await?;
    assert_eq!(Some(logs[0].clone()), prev);

    // insert and override nothing

    let override_nothing = Entry {
        log_id: LogId {
            term: 10,
            index: 100,
        },
        payload: EntryPayload::Blank,
    };

    let prev = tree
        .insert_value::<sled_key_space::Logs>(&override_nothing)
        .await?;
    assert_eq!(None, prev);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_contains_key() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    assert!(tree.get::<sled_key_space::Logs>(&5)?.is_none());

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    assert!(!tree.contains_key::<sled_key_space::Logs>(&1)?);
    assert!(tree.contains_key::<sled_key_space::Logs>(&2)?);
    assert!(!tree.contains_key::<sled_key_space::Logs>(&3)?);
    assert!(tree.contains_key::<sled_key_space::Logs>(&4)?);
    assert!(!tree.contains_key::<sled_key_space::Logs>(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_update_and_fetch() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    let v = tree
        .update_and_fetch::<sled_key_space::Files, _>(&"foo".to_string(), |v| {
            Some(v.unwrap_or_default() + "1")
        })
        .await?;
    assert_eq!(Some("1".to_string()), v);

    let v = tree
        .update_and_fetch::<sled_key_space::Files, _>(&"foo".to_string(), |v| {
            Some(v.unwrap_or_default() + "1")
        })
        .await?;
    assert_eq!(Some("11".to_string()), v);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_get() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    assert!(tree.get::<sled_key_space::Logs>(&5)?.is_none());

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    assert_eq!(None, tree.get::<sled_key_space::Logs>(&1)?);
    assert_eq!(Some(logs[0].clone()), tree.get::<sled_key_space::Logs>(&2)?);
    assert_eq!(None, tree.get::<sled_key_space::Logs>(&3)?);
    assert_eq!(Some(logs[1].clone()), tree.get::<sled_key_space::Logs>(&4)?);
    assert_eq!(None, tree.get::<sled_key_space::Logs>(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_last() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    // This test assumes the following order.
    // To ensure a last() does not returns item from another key space with smaller prefix
    assert!(sled_key_space::Logs::PREFIX < StateMachineMeta::PREFIX);

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    assert!(tree.last::<sled_key_space::Logs>()?.is_none());

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;
    assert_eq!(None, tree.last::<StateMachineMeta>()?);

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(LogId { term: 1, index: 2 }),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];

    tree.append::<StateMachineMeta>(metas.as_slice()).await?;

    assert_eq!(
        Some((4, logs[1].clone())),
        tree.last::<sled_key_space::Logs>()?
    );
    assert_eq!(
        Some((Initialized, StateMachineMetaValue::Bool(true))),
        tree.last::<StateMachineMeta>()?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_remove() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Blank,
        },
    ];

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

    let removed = tree.remove::<sled_key_space::Logs>(&0, false).await?;
    assert_eq!(None, removed);
    assert_eq!(logs[..], tree.range_values::<sled_key_space::Logs, _>(0..)?);

    // remove other key space
    let removed = tree.remove::<sled_key_space::Nodes>(&0, false).await?;
    assert_eq!(None, removed);
    assert_eq!(logs[..], tree.range_values::<sled_key_space::Logs, _>(0..)?);

    let removed = tree.remove::<sled_key_space::Logs>(&2, false).await?;
    assert_eq!(Some(logs[0].clone()), removed);
    assert_eq!(
        logs[1..],
        tree.range_values::<sled_key_space::Logs, _>(0..)?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_range_remove() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;
    tree.range_remove::<sled_key_space::Logs, _>(0.., false)
        .await?;
    assert_eq!(
        logs[5..],
        tree.range_values::<sled_key_space::Logs, _>(0..)?
    );

    tree.append_values::<sled_key_space::Logs>(&logs).await?;
    tree.range_remove::<sled_key_space::Logs, _>(1.., false)
        .await?;
    assert_eq!(
        logs[5..],
        tree.range_values::<sled_key_space::Logs, _>(0..)?
    );

    tree.append_values::<sled_key_space::Logs>(&logs).await?;
    tree.range_remove::<sled_key_space::Logs, _>(3.., true)
        .await?;
    assert_eq!(
        logs[0..1],
        tree.range_values::<sled_key_space::Logs, _>(0..)?
    );

    tree.append_values::<sled_key_space::Logs>(&logs).await?;
    tree.range_remove::<sled_key_space::Logs, _>(3..10, true)
        .await?;
    assert_eq!(
        logs[0..1],
        tree.range_values::<sled_key_space::Logs, _>(0..5)?
    );
    assert_eq!(
        logs[3..],
        tree.range_values::<sled_key_space::Logs, _>(5..)?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sledtree_multi_types() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;

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

    tree.append_values::<sled_key_space::Logs>(&logs).await?;

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
        let got = tree.range_values::<sled_key_space::Logs, _>(..)?;
        assert_eq!(logs, got);

        let got = tree.range_values::<StateMachineMeta, _>(..=LastApplied)?;
        assert_eq!(
            vec![StateMachineMetaValue::LogId(LogId { term: 1, index: 2 })],
            got
        );

        let got = tree.range_values::<StateMachineMeta, _>(Initialized..)?;
        assert_eq!(vec![StateMachineMetaValue::Bool(true)], got);

        let got = tree.range_keys::<StateMachineMeta, _>(Initialized..)?;
        assert_eq!(vec![Initialized], got);
    }

    // range remove are limited to its own namespace.
    {
        tree.range_remove::<StateMachineMeta, _>(.., false).await?;

        let got = tree.range_values::<sled_key_space::Logs, _>(..)?;
        assert_eq!(logs, got);
    }

    Ok(())
}

// --- key space test ---

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_append() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

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

    log_tree.append(&logs).await?;

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

    let got = log_tree.range_values(0..)?;
    assert_eq!(want, got);

    let got = log_tree.range_values(0..=5)?;
    assert_eq!(want[0..1], got);

    let got = log_tree.range_values(6..9)?;
    assert_eq!(want[1..], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_append_values_and_range_get() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

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

    log_tree.append_values(&logs).await?;

    let got = log_tree.range_values(0..)?;
    assert_eq!(logs, got);

    let got = log_tree.range_values(0..=2)?;
    assert_eq!(logs[0..1], got);

    let got = log_tree.range_values(0..3)?;
    assert_eq!(logs[0..1], got);

    let got = log_tree.range_values(0..5)?;
    assert_eq!(logs[0..2], got);

    let got = log_tree.range_values(0..10)?;
    assert_eq!(logs[0..3], got);

    let got = log_tree.range_values(0..11)?;
    assert_eq!(logs[0..4], got);

    let got = log_tree.range_values(9..11)?;
    assert_eq!(logs[2..4], got);

    let got = log_tree.range_values(10..256)?;
    assert_eq!(logs[3..4], got);

    let got = log_tree.range_values(10..257)?;
    assert_eq!(logs[3..5], got);

    let got = log_tree.range_values(257..)?;
    assert_eq!(logs[5..], got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_range_keys() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

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

    log_tree.append_values(&logs).await?;

    let got = log_tree.range_keys(0..)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = log_tree.range_keys(0..=2)?;
    assert_eq!(vec![2], got);

    let got = log_tree.range_keys(0..3)?;
    assert_eq!(vec![2], got);

    let got = log_tree.range_keys(0..10)?;
    assert_eq!(vec![2, 9], got);

    let got = log_tree.range_keys(0..11)?;
    assert_eq!(vec![2, 9, 10], got);

    let got = log_tree.range_keys(9..11)?;
    assert_eq!(vec![9, 10], got);

    let got = log_tree.range_keys(10..256)?;
    assert_eq!(vec![10], got);

    let got = log_tree.range_keys(11..)?;
    assert_eq!(Vec::<LogIndex>::new(), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_range_kvs() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

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

    log_tree.append_values(&logs).await?;

    let got = log_tree.range_kvs(9..)?;
    assert_eq!(vec![(9, logs[1].clone()), (10, logs[2].clone()),], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_scan_prefix() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let file_tree = tree.key_space::<sled_key_space::Files>();
    let kv_tree = tree.key_space::<sled_key_space::GenericKV>();

    let files: Vec<(String, String)> = vec![
        ("a".to_string(), "x".to_string()),
        ("ab".to_string(), "xy".to_string()),
        ("abc".to_string(), "xyz".to_string()),
        ("abd".to_string(), "xyZ".to_string()),
        ("b".to_string(), "y".to_string()),
    ];

    file_tree.append(&files).await?;

    let kvs = vec![
        ("a".to_string(), (1, KVValue::default())),
        ("ab".to_string(), (2, KVValue::default())),
        ("b".to_string(), (3, KVValue::default())),
    ];

    kv_tree.append(&kvs).await?;

    let got = file_tree.scan_prefix(&"".to_string())?;
    assert_eq!(files, got);

    let got = file_tree.scan_prefix(&"ab".to_string())?;
    assert_eq!(files[1..4], got);

    let got = kv_tree.scan_prefix(&"".to_string())?;
    assert_eq!(kvs, got);

    let got = kv_tree.scan_prefix(&"ab".to_string())?;
    assert_eq!(kvs[1..2], got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_insert() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

    assert_eq!(None, log_tree.get(&5)?);

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
        log_tree.insert_value(log).await?;
    }

    assert_eq!(logs, log_tree.range_values(..)?);

    // insert and override

    let override_2 = Entry {
        log_id: LogId { term: 10, index: 2 },
        payload: EntryPayload::Blank,
    };

    let prev = log_tree.insert_value(&override_2).await?;
    assert_eq!(Some(logs[0].clone()), prev);

    // insert and override nothing

    let override_nothing = Entry {
        log_id: LogId {
            term: 10,
            index: 100,
        },
        payload: EntryPayload::Blank,
    };

    let prev = log_tree.insert_value(&override_nothing).await?;
    assert_eq!(None, prev);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_contains_key() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

    assert_eq!(None, log_tree.get(&5)?);

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

    log_tree.append_values(&logs).await?;

    assert!(!log_tree.contains_key(&1)?);
    assert!(log_tree.contains_key(&2)?);
    assert!(!log_tree.contains_key(&3)?);
    assert!(log_tree.contains_key(&4)?);
    assert!(!log_tree.contains_key(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_update_and_fetch() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let file_tree = tree.key_space::<sled_key_space::Files>();

    let v = file_tree
        .update_and_fetch(&"foo".to_string(), |v| Some(v.unwrap_or_default() + "1"))
        .await?;
    assert_eq!(Some("1".to_string()), v);

    let v = file_tree
        .update_and_fetch(&"foo".to_string(), |v| Some(v.unwrap_or_default() + "1"))
        .await?;
    assert_eq!(Some("11".to_string()), v);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_get() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

    assert_eq!(None, log_tree.get(&5)?);

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

    log_tree.append_values(&logs).await?;

    assert_eq!(None, log_tree.get(&1)?);
    assert_eq!(Some(logs[0].clone()), log_tree.get(&2)?);
    assert_eq!(None, log_tree.get(&3)?);
    assert_eq!(Some(logs[1].clone()), log_tree.get(&4)?);
    assert_eq!(None, log_tree.get(&5)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_last() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

    assert_eq!(None, log_tree.last()?);

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

    log_tree.append_values(&logs).await?;
    assert_eq!(Some((4, logs[1].clone())), log_tree.last()?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_remove() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

    let logs: Vec<Entry<LogEntry>> = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Blank,
        },
    ];

    log_tree.append_values(&logs).await?;

    let removed = log_tree.remove(&0, false).await?;
    assert_eq!(None, removed);
    assert_eq!(logs[..], log_tree.range_values(0..)?);

    let removed = log_tree.remove(&2, false).await?;
    assert_eq!(Some(logs[0].clone()), removed);
    assert_eq!(logs[1..], log_tree.range_values(0..)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_range_remove() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();

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

    log_tree.append_values(&logs).await?;
    log_tree.range_remove(0.., false).await?;
    assert_eq!(logs[5..], log_tree.range_values(0..)?);

    log_tree.append_values(&logs).await?;
    log_tree.range_remove(1.., false).await?;
    assert_eq!(logs[5..], log_tree.range_values(0..)?);

    log_tree.append_values(&logs).await?;
    log_tree.range_remove(3.., true).await?;
    assert_eq!(logs[0..1], log_tree.range_values(0..)?);

    log_tree.append_values(&logs).await?;
    log_tree.range_remove(3..10, true).await?;
    assert_eq!(logs[0..1], log_tree.range_values(0..5)?);
    assert_eq!(logs[3..], log_tree.range_values(5..)?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_as_multi_types() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.config.tree_name("foo"), true).await?;
    let log_tree = tree.key_space::<sled_key_space::Logs>();
    let sm_meta = tree.key_space::<StateMachineMeta>();

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

    log_tree.append_values(&logs).await?;

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(LogId { term: 1, index: 2 }),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];
    sm_meta.append(&metas).await?;

    // range get/keys are limited to its own namespace.
    {
        let got = log_tree.range_values(..)?;
        assert_eq!(logs, got);

        let got = sm_meta.range_values(..=LastApplied)?;
        assert_eq!(
            vec![StateMachineMetaValue::LogId(LogId { term: 1, index: 2 })],
            got
        );

        let got = sm_meta.range_values(Initialized..)?;
        assert_eq!(vec![StateMachineMetaValue::Bool(true)], got);

        let got = sm_meta.range_keys(Initialized..)?;
        assert_eq!(vec![Initialized], got);
    }

    // range remove are limited to its own namespace.
    {
        sm_meta.range_remove(.., false).await?;

        let got = log_tree.range_values(..)?;
        assert_eq!(logs, got);
    }

    Ok(())
}
