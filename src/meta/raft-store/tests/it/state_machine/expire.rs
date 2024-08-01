// Copyright 2022 Datafuse Labs.
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

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_meta_raft_store::key_spaces::Expire;
use databend_common_meta_raft_store::key_spaces::GenericKV;
use databend_common_meta_raft_store::state_machine::ExpireKey;
use databend_common_meta_raft_store::state_machine::StateMachine;
use databend_common_meta_sled_store::AsKeySpace;
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use test_harness::test;

use crate::testing::new_raft_test_context;
use crate::testing::raft_store_test_harness;

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_state_machine_update_expiration_index() -> anyhow::Result<()> {
    // - Update expiration index when upsert.
    // - Remove from expiration index when overriding
    // - Remove from expiration index when log-time is provided.

    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 0).await?;
    let expires = sm.sm_tree.key_space::<Expire>();

    let ls_expire = |expire_key_space: &AsKeySpace<Expire>| {
        expire_key_space
            .range(..)
            .unwrap()
            .map(|item_res| {
                let item = item_res.unwrap();
                (item.key().unwrap(), item.value().unwrap().key)
            })
            .collect::<Vec<_>>()
    };

    let s = |x: &str| x.to_string();

    let now = now();

    sm.apply(&ent(3, "a", Some(now - 1), None)).await?;
    assert_eq!(
        vec![(ExpireKey::new((now - 1) * 1000, 1), s("a"))],
        ls_expire(&expires)
    );

    // override
    sm.apply(&ent(4, "a", Some(now - 2), None)).await?;
    assert_eq!(
        vec![(ExpireKey::new((now - 2) * 1000, 2), s("a"))],
        ls_expire(&expires)
    );

    // Add another expiration index
    sm.apply(&ent(5, "b", Some(now - 2), None)).await?;
    assert_eq!(
        vec![
            //
            (ExpireKey::new((now - 2) * 1000, 2), s("a")),
            (ExpireKey::new((now - 2) * 1000, 3), s("b")),
        ],
        ls_expire(&expires)
    );

    // Clean expired
    sm.apply(&ent(6, "a", Some(now + 1), Some(now * 1000)))
        .await?;
    assert_eq!(
        vec![
            //
            (ExpireKey::new((now + 1) * 1000, 4), s("a")),
        ],
        ls_expire(&expires)
    );
    let kvs = sm.sm_tree.key_space::<GenericKV>();
    assert!(kvs.get(&s("b"))?.is_none());

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_state_machine_list_expired() -> anyhow::Result<()> {
    // - Feed logs into state machine.
    // - List expired keys

    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 0).await?;

    let now = now();

    let logs = vec![
        ent(3, "a", Some(now - 1), None), //
        ent(4, "b", Some(now - 2), None),
        ent(5, "c", Some(now + 20), None),
        ent(6, "d", Some(now - 3), None),
    ];

    for l in logs.iter() {
        sm.apply(l).await?;
    }

    let ls = sm.list_expired_kvs(now * 1000)?;

    assert_eq!(
        vec![
            expired_item("d", now - 3, 4), //
            expired_item("b", now - 2, 2),
            expired_item("a", now - 1, 1),
        ],
        ls
    );

    Ok(())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Build an item returned by `list_expired_kvs()`.
fn expired_item(key: &str, time_sec: u64, seq: u64) -> (String, ExpireKey) {
    (key.to_string(), ExpireKey::new(time_sec * 1000, seq))
}

/// Build a raft log entry with expire time
fn ent(index: u64, key: &str, expire: Option<u64>, time_ms: Option<u64>) -> Entry {
    Entry {
        log_id: new_log_id(1, 0, index),
        payload: EntryPayload::Normal(LogEntry {
            txid: None,
            time_ms,
            cmd: Cmd::UpsertKV(
                UpsertKV::update(key, key.as_bytes()).with(MetaSpec::new(expire, None)),
            ),
        }),
    }
}
