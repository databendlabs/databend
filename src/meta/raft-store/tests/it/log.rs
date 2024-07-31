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

use databend_common_meta_raft_store::log::RaftLog;
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::UpsertKV;
use test_harness::test;

use crate::testing::new_raft_test_context;
use crate::testing::raft_store_test_harness;

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_raft_log_open() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let db = &tc.db;
    RaftLog::open(db, &tc.raft_config).await?;

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_raft_log_append_and_range_get() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db, &tc.raft_config).await?;

    let logs: Vec<Entry> = vec![
        Entry {
            log_id: new_log_id(1, 0, 2),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(3, 0, 4),
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
        Entry {
            log_id: new_log_id(1, 0, 256),
            payload: EntryPayload::Blank,
        },
    ];

    rl.append(logs.clone()).await?;

    let got = rl.range_values(0..)?;
    assert_eq!(logs, got);

    let got = rl.range_values(0..=2)?;
    assert_eq!(logs[0..1], got);

    let got = rl.range_values(0..3)?;
    assert_eq!(logs[0..1], got);

    let got = rl.range_values(0..5)?;
    assert_eq!(logs[0..2], got);

    let got = rl.range_values(0..10)?;
    assert_eq!(logs[0..3], got);

    let got = rl.range_values(0..11)?;
    assert_eq!(logs[0..4], got);

    let got = rl.range_values(9..11)?;
    assert_eq!(logs[2..4], got);

    let got = rl.range_values(10..256)?;
    assert_eq!(logs[3..4], got);

    let got = rl.range_values(10..257)?;
    assert_eq!(logs[3..5], got);

    let got = rl.range_values(257..)?;
    assert_eq!(logs[5..], got);
    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_raft_log_insert() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db, &tc.raft_config).await?;

    assert_eq!(None, rl.logs().get(&5)?);

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

    rl.append(logs.clone()).await?;

    assert_eq!(logs, rl.range_values(..)?);

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_raft_log_get() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db, &tc.raft_config).await?;

    assert_eq!(None, rl.logs().get(&5)?);

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

    rl.append(logs.clone()).await?;

    assert_eq!(None, rl.logs().get(&1)?);
    assert_eq!(Some(logs[0].clone()), rl.logs().get(&2)?);
    assert_eq!(None, rl.logs().get(&3)?);
    assert_eq!(Some(logs[1].clone()), rl.logs().get(&4)?);
    assert_eq!(None, rl.logs().get(&5)?);

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_raft_log_last() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db, &tc.raft_config).await?;

    assert_eq!(None, rl.logs().last()?);

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

    rl.append(logs.clone()).await?;
    assert_eq!(Some((4, logs[1].clone())), rl.logs().last()?);

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_raft_log_range_remove() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let db = &tc.db;
    let rl = RaftLog::open(db, &tc.raft_config).await?;

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

    rl.append(logs.clone()).await?;
    rl.range_remove(0..).await?;
    assert_eq!(logs[5..], rl.range_values(0..)?);

    rl.append(logs.clone()).await?;
    rl.range_remove(1..).await?;
    assert_eq!(logs[5..], rl.range_values(0..)?);

    rl.append(logs.clone()).await?;
    rl.range_remove(3..).await?;
    assert_eq!(logs[0..1], rl.range_values(0..)?);

    rl.append(logs.clone()).await?;
    rl.range_remove(3..10).await?;
    assert_eq!(logs[0..1], rl.range_values(0..5)?);
    assert_eq!(logs[3..], rl.range_values(5..)?);

    Ok(())
}
