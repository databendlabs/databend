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
use async_raft::raft::EntryConfigChange;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use async_raft::LogId;
use common_metatypes::MatchSeq;
use common_metatypes::Operation;
use maplit::btreeset;
use sled::IVec;

use crate::meta_service::state_machine::SnapshotKeyValue;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;

/// Logs and the expected snapshot for testing snapshot.
pub fn snapshot_logs() -> (Vec<Entry<LogEntry>>, Vec<String>) {
    let logs = vec![
        Entry {
            log_id: LogId { term: 1, index: 1 },
            payload: EntryPayload::ConfigChange(EntryConfigChange {
                membership: MembershipConfig {
                    members: btreeset![1, 2, 3],
                    members_after_consensus: None,
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 4 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::UpsertKV {
                        key: "a".to_string(),
                        seq: MatchSeq::Any,
                        value: Operation::Update(b"A".to_vec()),
                        value_meta: None,
                    },
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 5 },
            payload: EntryPayload::ConfigChange(EntryConfigChange {
                membership: MembershipConfig {
                    members: btreeset![4, 5, 6],
                    members_after_consensus: None,
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 6 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::SetFile {
                        key: "b".to_string(),
                        value: "B".to_string(),
                    },
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 8 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::IncrSeq {
                        key: "c".to_string(),
                    },
                },
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 9 },
            payload: EntryPayload::Normal(EntryNormal {
                data: LogEntry {
                    txid: None,
                    cmd: Cmd::AddNode {
                        node_id: 5,
                        node: Default::default(),
                    },
                },
            }),
        },
    ];
    let want = vec![
        "[2, 0, 0, 0, 0, 0, 0, 0, 5]:{\"name\":\"\",\"address\":\"\"}", // Nodes
        "[3, 1]:{\"LogId\":{\"term\":1,\"index\":9}}",                  // sm meta: LastApplied
        "[3, 2]:{\"Bool\":true}",                                       // sm meta: init
        "[3, 3]:{\"Membership\":{\"members\":[4,5,6],\"members_after_consensus\":null}}", // membership
        "[5, 98]:B",                                                                      // Files
        "[6, 97]:[1,{\"meta\":null,\"value\":[65]}]", // generic kv
        "[7, 99]:1",                                  // sequence: c
        "[7, 103, 101, 110, 101, 114, 105, 99, 95, 107, 118]:1", // sequence: by upsertkv
    ]
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<_>>();

    (logs, want)
}

pub fn pretty_snapshot(snap: &Vec<SnapshotKeyValue>) -> Vec<String> {
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
