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

use databend_common_meta_sled_store::SledTree;
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::LogIndex;
use databend_common_meta_types::UpsertKV;
use test_harness::test;

use crate::testing::fake_key_spaces::Files;
use crate::testing::fake_key_spaces::GenericKV;
use crate::testing::fake_key_spaces::Logs;
use crate::testing::fake_key_spaces::StateMachineMeta;
use crate::testing::fake_state_machine_meta::StateMachineMetaKey::Initialized;
use crate::testing::fake_state_machine_meta::StateMachineMetaKey::LastApplied;
use crate::testing::fake_state_machine_meta::StateMachineMetaValue;
use crate::testing::new_sled_test_context;
use crate::testing::sled_test_harness;

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_sled_tree_open() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    SledTree::open(db, tc.tree_name, true)?;

    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_as_range() -> anyhow::Result<()> {
    // This test assumes the following order.
    // to check the range boundary.
    // assert!(Logs::PREFIX < StateMachineMeta::PREFIX);

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();
    let meta_tree = tree.key_space::<StateMachineMeta>();

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
    ];

    log_tree.append(logs.clone()).await?;

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(new_log_id(1, 0, 2)),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];

    meta_tree.append(metas.clone()).await?;

    // key space Logs

    let mut it = log_tree.range(..)?;
    assert_eq!((2, logs[0].clone()), it.next().unwrap()?.kv()?);
    assert_eq!((4, logs[1].clone()), it.next().unwrap()?.kv()?);
    assert!(it.next().is_none());

    // key space Logs reversed

    let mut it = log_tree.range(..)?.rev();
    assert_eq!((4, logs[1].clone()), it.next().unwrap()?.kv()?);
    assert_eq!((2, logs[0].clone()), it.next().unwrap()?.kv()?);
    assert!(it.next().is_none());

    // key space StateMachineMeta

    let mut it = meta_tree.range(..)?;
    assert_eq!(metas[0], it.next().unwrap()?.kv()?);
    assert_eq!(metas[1], it.next().unwrap()?.kv()?);
    assert!(it.next().is_none());

    // key space StateMachineMeta reversed

    let mut it = meta_tree.range(..)?.rev();
    assert_eq!(metas[1], it.next().unwrap()?.kv()?);
    assert_eq!(metas[0], it.next().unwrap()?.kv()?);
    assert!(it.next().is_none());
    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_last() -> anyhow::Result<()> {
    // This test assumes the following order.
    // To ensure a last() does not returns item from another key space with smaller prefix
    // assert!(Logs::PREFIX < StateMachineMeta::PREFIX);

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    assert_eq!(None, log_tree.last()?);

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
    ];

    log_tree.append(logs.clone()).await?;
    assert_eq!(Some((4, logs[1].clone())), log_tree.last()?);
    assert_eq!(None, tree.key_space::<StateMachineMeta>().last()?);

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(new_log_id(1, 0, 2)),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];

    tree.key_space::<StateMachineMeta>()
        .append(metas.clone())
        .await?;

    assert_eq!(Some((4, logs[1].clone())), log_tree.last()?);
    assert_eq!(
        Some((Initialized, StateMachineMetaValue::Bool(true))),
        tree.key_space::<StateMachineMeta>().last()?
    );

    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_append() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    let logs: Vec<(LogIndex, Entry)> = vec![
        (8, Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        }),
        (5, Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        }),
    ];

    log_tree.append(logs.clone()).await?;

    let want: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
        Entry {
            log_id: new_log_id(1, 0, 2),
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

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_append_and_range_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
        Entry {
            log_id: new_log_id(1, 0, 9),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 10),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 256),
            payload: EntryPayload::Blank,
        },
    ];

    log_tree.append(logs.clone()).await?;

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

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_range_kvs() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 9),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 10),
            payload: EntryPayload::Blank,
        },
    ];

    log_tree.append(logs.clone()).await?;

    let got = log_tree
        .range(9..)?
        .map(|x| x.unwrap().kv().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(vec![(9, logs[1].clone()), (10, logs[2].clone()),], got);

    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_scan_prefix() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let file_tree = tree.key_space::<Files>();
    let kv_tree = tree.key_space::<GenericKV>();

    let files: Vec<(String, String)> = vec![
        ("a".to_string(), "x".to_string()),
        ("ab".to_string(), "xy".to_string()),
        ("abc".to_string(), "xyz".to_string()),
        ("abd".to_string(), "xyZ".to_string()),
        ("b".to_string(), "y".to_string()),
    ];

    file_tree.append(files.clone()).await?;

    let kvs = vec![
        ("a".to_string(), SeqV::new(1, vec![])),
        ("ab".to_string(), SeqV::new(2, vec![])),
        ("b".to_string(), SeqV::new(3, vec![])),
    ];

    kv_tree.append(kvs.clone()).await?;

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

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_insert() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    assert_eq!(None, log_tree.get(&5)?);

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
    ];

    log_tree.append(logs.clone()).await?;

    assert_eq!(logs, log_tree.range_values(..)?);

    // insert and override

    let override_2 = Entry {
        log_id: new_log_id(10, 0, 2),
        payload: EntryPayload::Blank,
    };

    let prev = log_tree
        .insert(&override_2.log_id.index, &override_2)
        .await?;
    assert_eq!(Some(logs[0].clone()), prev);

    // insert and override nothing

    let override_nothing = Entry {
        log_id: new_log_id(10, 0, 100),
        payload: EntryPayload::Blank,
    };

    let prev = log_tree
        .insert(&override_nothing.log_id.index, &override_nothing)
        .await?;
    assert_eq!(None, prev);

    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_get() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    assert_eq!(None, log_tree.get(&5)?);

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
    ];

    log_tree.append(logs.clone()).await?;

    assert_eq!(None, log_tree.get(&1)?);
    assert_eq!(Some(logs[0].clone()), log_tree.get(&2)?);
    assert_eq!(None, log_tree.get(&3)?);
    assert_eq!(Some(logs[1].clone()), log_tree.get(&4)?);
    assert_eq!(None, log_tree.get(&5)?);

    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_range_remove() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
        Entry {
            log_id: new_log_id(1, 0, 9),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 10),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 256),
            payload: EntryPayload::Blank,
        },
    ];

    log_tree.append(logs.clone()).await?;
    log_tree.range_remove(0.., false).await?;
    assert_eq!(logs[5..], log_tree.range_values(0..)?);

    log_tree.append(logs.clone()).await?;
    log_tree.range_remove(1.., false).await?;
    assert_eq!(logs[5..], log_tree.range_values(0..)?);

    log_tree.append(logs.clone()).await?;
    log_tree.range_remove(3.., true).await?;
    assert_eq!(logs[0..1], log_tree.range_values(0..)?);

    log_tree.append(logs.clone()).await?;
    log_tree.range_remove(3..10, true).await?;
    assert_eq!(logs[0..1], log_tree.range_values(0..5)?);
    assert_eq!(logs[3..], log_tree.range_values(5..)?);

    Ok(())
}

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_key_space_multi_types() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();
    let sm_meta = tree.key_space::<StateMachineMeta>();

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,

                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
    ];

    log_tree.append(logs.clone()).await?;

    let metas = vec![
        (
            LastApplied,
            StateMachineMetaValue::LogId(new_log_id(1, 0, 2)),
        ),
        (Initialized, StateMachineMetaValue::Bool(true)),
    ];
    sm_meta.append(metas.clone()).await?;

    // range get/keys are limited to its own namespace.
    {
        let got = log_tree.range_values(..)?;
        assert_eq!(logs, got);

        let got = sm_meta.range_values(..=LastApplied)?;
        assert_eq!(vec![StateMachineMetaValue::LogId(new_log_id(1, 0, 2))], got);

        let got = sm_meta.range_values(Initialized..)?;
        assert_eq!(vec![StateMachineMetaValue::Bool(true)], got);

        let got = sm_meta
            .range(Initialized..)?
            .map(|x| x.unwrap().key().unwrap())
            .collect::<Vec<_>>();
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

#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_export() -> anyhow::Result<()> {
    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;
    let log_tree = tree.key_space::<Logs>();

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,
                cmd: Cmd::UpsertKV(UpsertKV::insert("foo", b"foo")),
            }),
        },
    ];

    log_tree.append(logs.clone()).await?;

    let data = tree.export()?;

    for kv in data.iter() {
        println!("{:?}", kv);
    }

    Ok(())
}
