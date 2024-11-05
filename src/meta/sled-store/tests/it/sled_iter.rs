// Copyright 2023 Datafuse Labs.
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

use databend_common_meta_sled_store::SledItem;
use databend_common_meta_sled_store::SledTree;
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::UpsertKV;
use log::info;
use pretty_assertions::assert_eq;
use sled::IVec;
use test_harness::test;

use crate::testing::fake_key_spaces::Logs;
use crate::testing::new_sled_test_context;
use crate::testing::sled_test_harness;

/// Feed some data to two trees, iterate them and check output.
#[test(harness = sled_test_harness)]
#[fastrace::trace]
async fn test_sled_iter() -> anyhow::Result<()> {
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

    info!("--- init some data");
    let t1 = {
        let tc = new_sled_test_context();

        let tree = SledTree::open(&tc.db, tc.tree_name.clone(), true)?;
        let log_tree = tree.key_space::<Logs>();

        log_tree.append(logs.clone()).await?;
        tc.tree_name
    };

    let t2 = {
        let tc = new_sled_test_context();

        let tree = SledTree::open(&tc.db, tc.tree_name.clone(), true)?;
        let log_tree = tree.key_space::<Logs>();

        log_tree.append(logs.clone()).await?;
        tc.tree_name
    };

    // Iterator output IVec

    let mut trees = vec![t1.clone(), t2.clone()];

    for tree_iter in databend_common_meta_sled_store::iter::<IVec>() {
        let (tree_name, item_iter) = tree_iter?;

        if tree_name == "__sled__default" {
            continue;
        }

        if !trees.contains(&tree_name) {
            // When tests run concurrently, there are other trees created by other test case.
            continue;
        }

        assert_eq!(trees.remove(0), tree_name);

        let mut got = vec![];
        for item in item_iter {
            let (k, v) = item?;

            let item = SledItem::<Logs>::new(k, v);
            let line = format!("{}, {:?}", item.key()?, item.value()?);
            got.push(line);
        }

        let want = vec![
            "2, Entry { log_id: LogId { leader_id: LeaderId { term: 1, node_id: 0 }, index: 2 }, payload: blank }".to_string(),
            "4, Entry { log_id: LogId { leader_id: LeaderId { term: 3, node_id: 0 }, index: 4 }, payload: normal }".to_string(),
        ];

        assert_eq!(want, got);
    }

    // Iterator outputs Vec<u8>

    let trees = [t1, t2];

    let mut got = vec![];
    for tree_iter in databend_common_meta_sled_store::iter::<Vec<u8>>() {
        let (tree_name, item_iter) = tree_iter?;

        if !trees.contains(&tree_name) {
            // When tests run concurrently, there are other trees created by other test case.
            continue;
        }

        for item in item_iter {
            let (k, v) = item?;

            let line = format!("{:?}, {}", k, String::from_utf8(v).unwrap());
            got.push(line);
        }
    }

    let want = vec![
        r#"[1, 0, 0, 0, 0, 0, 0, 0, 2], {"log_id":{"leader_id":{"term":1,"node_id":0},"index":2},"payload":"Blank"}"#,
        r#"[1, 0, 0, 0, 0, 0, 0, 0, 4], {"log_id":{"leader_id":{"term":3,"node_id":0},"index":4},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"foo","seq":{"Exact":0},"value":{"Update":[102,111,111]},"value_meta":null}}}}}"#,
        r#"[1, 0, 0, 0, 0, 0, 0, 0, 2], {"log_id":{"leader_id":{"term":1,"node_id":0},"index":2},"payload":"Blank"}"#,
        r#"[1, 0, 0, 0, 0, 0, 0, 0, 4], {"log_id":{"leader_id":{"term":3,"node_id":0},"index":4},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"foo","seq":{"Exact":0},"value":{"Update":[102,111,111]},"value_meta":null}}}}}"#,
    ];

    assert_eq!(want, got);

    Ok(())
}
