// Copyright 2021 Datafuse Labs.
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

use common_meta_sled_store::openraft;
use common_meta_sled_store::sled;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::LogId;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::RaftTxId;
use maplit::btreeset;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;
use openraft::Membership;
use sled::IVec;

use crate::state_machine::SnapshotKeyValue;

/// Logs and the expected snapshot for testing snapshot.
pub fn snapshot_logs() -> (Vec<Entry<LogEntry>>, Vec<String>) {
    let logs = vec![
        Entry {
            log_id: LogId { term: 1, index: 1 },
            payload: EntryPayload::Membership(Membership::new_single(btreeset![1, 2, 3])),
        },
        Entry {
            log_id: LogId { term: 1, index: 4 },
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: "a".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"A".to_vec()),
                    value_meta: None,
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 5 },
            payload: EntryPayload::Membership(Membership::new_single(btreeset![4, 5, 6])),
        },
        Entry {
            log_id: LogId { term: 1, index: 6 },
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: LogId { term: 1, index: 8 },
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                cmd: Cmd::IncrSeq {
                    key: "c".to_string(),
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Normal(LogEntry {
                txid: None,
                cmd: Cmd::AddNode {
                    node_id: 5,
                    node: Default::default(),
                },
            }),
        },
    ];
    let want = vec![
        "[2, 0, 0, 0, 0, 0, 0, 0, 5]:{\"name\":\"\",\"address\":\"\"}", // Nodes
        "[3, 1]:{\"LogId\":{\"term\":1,\"index\":9}}",                  // sm meta: LastApplied
        "[3, 2]:{\"Bool\":true}",                                       // sm meta: init
        "[3, 3]:{\"Membership\":{\"log_id\":{\"term\":1,\"index\":5},\"membership\":{\"configs\":[[4,5,6]],\"all_nodes\":[4,5,6]}}}", // membership
        "[6, 97]:{\"seq\":1,\"meta\":null,\"data\":[65]}", // generic kv
        "[7, 99]:1",                                       // sequence: c
        "[7, 103, 101, 110, 101, 114, 105, 99, 45, 107, 118]:1", // sequence: by upsertkv
    ]
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

pub fn pretty_snapshot_iter(snap: impl Iterator<Item = sled::Result<(IVec, IVec)>>) -> Vec<String> {
    let mut res = vec![];
    for kv in snap {
        let (k, v) = kv.unwrap();
        let line = format!("{:?}:{}", k, String::from_utf8(v.to_vec()).unwrap());
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

pub type AddFileCase = (
    &'static str,
    Option<RaftTxId>,
    &'static str,
    &'static str,
    Option<String>,
    Option<String>,
);

// test cases for Cmd::AddFile
// case_name, txid, key, value, want_prev, want_result
pub fn cases_add_file() -> Vec<AddFileCase> {
    vec![
        (
            "add on none",
            Some(RaftTxId::new("foo", 1)),
            "k1",
            "v1",
            None,
            Some("v1".to_string()),
        ),
        (
            "add on existent",
            Some(RaftTxId::new("foo", 2)),
            "k1",
            "v2",
            Some("v1".to_string()),
            None,
        ),
        (
            "dup set with same serial, even with diff key, got the previous result",
            Some(RaftTxId::new("foo", 2)),
            "k2",
            "v3",
            Some("v1".to_string()),
            None,
        ),
        (
            "diff client, same serial",
            Some(RaftTxId::new("bar", 2)),
            "k2",
            "v3",
            None,
            Some("v3".to_string()),
        ),
        ("no txid", None, "k3", "v4", None, Some("v4".to_string())),
    ]
}
