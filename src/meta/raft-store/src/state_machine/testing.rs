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

use databend_common_meta_sled_store::openraft;
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::RaftTxId;
use databend_common_meta_types::UpsertKV;
use maplit::btreeset;
use openraft::Membership;

use crate::key_spaces::RaftStoreEntry;
use crate::state_machine::SnapshotKeyValue;

/// Logs and the expected snapshot for testing snapshot.
pub fn snapshot_logs() -> (Vec<Entry>, Vec<String>) {
    let logs = vec![
        Entry {
            log_id: new_log_id(1, 0, 1),
            payload: EntryPayload::Membership(Membership::new(vec![btreeset![1, 2, 3]], ())),
        },
        Entry {
            log_id: new_log_id(1, 0, 4),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,
                cmd: Cmd::UpsertKV(UpsertKV::update("a", b"A")),
            }),
        },
        Entry {
            log_id: new_log_id(1, 0, 5),
            payload: EntryPayload::Membership(Membership::new(vec![btreeset![4, 5, 6]], ())),
        },
        Entry {
            log_id: new_log_id(1, 0, 6),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 8),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 9),
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                time_ms: None,
                cmd: Cmd::AddNode {
                    node_id: 5,
                    node: Default::default(),
                    overriding: false,
                },
            }),
        },
    ];
    let want = [ //
        r#"{"Sequences":{"key":"generic-kv","value":1}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":1,"node_id":0},"index":9}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":5},"membership":{"configs":[[4,5,6]],"nodes":{"4":{},"5":{},"6":{}}}}}}}"#,
        r#"{"Nodes":{"key":5,"value":{"name":"","endpoint":{"addr":"","port":0},"grpc_api_advertise_address":null}}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":1,"meta":null,"data":[65]}}}"#]
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<_>>();

    (logs, want)
}

pub fn pretty_snapshot(snap: &[SnapshotKeyValue]) -> Vec<String> {
    let mut res = vec![];
    for kv in snap.iter() {
        let k = kv[0].clone();
        let v = kv[1].clone();
        let line = format!("{:?}:{}", k, String::from_utf8(v.to_vec()).unwrap());
        res.push(line);
    }
    res
}

pub fn pretty_snapshot_entries<'a>(
    snap: impl IntoIterator<Item = &'a RaftStoreEntry>,
) -> Vec<String> {
    let mut res = vec![];

    for kv in snap.into_iter() {
        let line = serde_json::to_string(kv).unwrap();
        res.push(line);
    }
    res
}

// test cases fro Cmd::IncrSeq:
// case_name, txid, key, want
pub fn cases_incr_seq() -> Vec<(&'static str, Option<RaftTxId>, &'static str, u64)> {
    vec![
        ("incr on none", Some(RaftTxId::new("foo", 1)), "k1", 1),
        ("incr on existent", Some(RaftTxId::new("foo", 2)), "k1", 2),
        (
            "dup: same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            2,
        ),
        (
            "diff client, same serial, not a dup request",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            1,
        ),
        ("no txid, no de-dup", None, "k2", 2),
    ]
}
