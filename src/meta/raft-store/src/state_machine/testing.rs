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
use databend_common_meta_types::raft_types::new_log_id;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::EntryPayload;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::UpsertKV;
use maplit::btreeset;
use openraft::entry::RaftEntry;
use openraft::Membership;

/// Logs and the expected snapshot for testing snapshot.
pub fn snapshot_logs() -> (Vec<Entry>, Vec<String>) {
    let logs = vec![
        Entry {
            log_id: new_log_id(1, 0, 1),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![btreeset![1, 2, 3]],
                [],
            )),
        },
        Entry::new_blank(new_log_id(1, 0, 2)),
        Entry::new_blank(new_log_id(1, 0, 3)),
        Entry {
            log_id: new_log_id(1, 0, 4),
            payload: EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
                "a", b"A",
            )))),
        },
        Entry {
            log_id: new_log_id(1, 0, 5),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![btreeset![4, 5, 6]],
                [],
            )),
        },
        Entry {
            log_id: new_log_id(1, 0, 6),
            payload: EntryPayload::Blank,
        },
        Entry::new_blank(new_log_id(1, 0, 7)),
        Entry {
            log_id: new_log_id(1, 0, 8),
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: new_log_id(1, 0, 9),
            payload: EntryPayload::Normal(LogEntry::new(Cmd::AddNode {
                node_id: 5,
                node: Default::default(),
                overriding: false,
            })),
        },
    ];
    let want = [ //
        r#"{"Sequences":{"key":"generic-kv","value":1}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":1,"node_id":0},"index":9}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":5},"membership":{"configs":[[4,5,6]],"nodes":{"4":{},"5":{},"6":{}}}}}}}"#,
        r#"{"Nodes":{"key":5,"value":{"name":"","endpoint":{"addr":"","port":0},"grpc_api_advertise_address":null}}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":1,"meta":{"proposed_at_ms":0},"data":[65]}}}"#]
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<_>>();

    (logs, want)
}
