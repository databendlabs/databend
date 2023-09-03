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

use std::sync::Arc;

use common_meta_types::SeqNum;
use common_meta_types::SeqV;
use common_meta_types::SnapshotMeta;
use futures::Stream;
use futures_util::StreamExt;

use crate::key_spaces::RaftStoreEntry;
use crate::ondisk::Header;
use crate::ondisk::OnDisk;
use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::marked::Marked;
use crate::state_machine::ExpireKey;
use crate::state_machine::ExpireValue;
use crate::state_machine::MetaSnapshotId;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

/// A snapshot view of a state machine, which is static and not affected by further writing to the state machine.
pub struct SnapshotViewV002 {
    /// The compacted snapshot data.
    top: Arc<Level>,

    /// Original non compacted snapshot data.
    ///
    /// This is kept just for debug.
    original: Arc<Level>,
}

impl SnapshotViewV002 {
    pub fn new(top: Arc<Level>) -> Self {
        Self {
            top: top.clone(),
            original: top,
        }
    }

    /// Return the data level of this snapshot
    pub fn top(&self) -> Arc<Level> {
        self.top.clone()
    }

    /// The original, non compacted snapshot data.
    pub fn original(&self) -> Arc<Level> {
        self.original.clone()
    }

    /// Extract metadata of the snapshot.
    ///
    /// Includes: last_applied, last_membership, snapshot_id.
    // TODO: let the caller specify snapshot id?
    pub fn build_snapshot_meta(&self) -> SnapshotMeta {
        // The top level contains all information we need to build snapshot meta.
        let top = self.top();
        let level_data = top.data_ref();

        let last_applied = *level_data.last_applied_ref();
        let last_membership = level_data.last_membership_ref().clone();

        let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);

        SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_id: last_applied,
            last_membership,
        }
    }

    /// Compact into one level and remove all tombstone record.
    pub async fn compact(&mut self) {
        // TODO: use a explicit method to return a compaction base
        let mut data = self.top.data_ref().new_level();

        // `range()` will compact tombstone internally
        let strm = MapApi::<String>::range::<String, _>(self.top.as_ref(), ..)
            .await
            .filter(|(_k, v)| {
                let x = !v.is_tomb_stone();
                async move { x }
            })
            .map(|(k, v)| (k.clone(), v.clone()));

        let btreemap = strm.collect().await;

        data.replace_kv(btreemap);

        // `range()` will compact tombstone internally
        let strm = MapApi::<ExpireKey>::range(self.top.as_ref(), ..)
            .await
            .filter(|(_k, v)| {
                let x = !v.is_tomb_stone();
                async move { x }
            })
            .map(|(k, v)| (k.clone(), v.clone()));

        let btreemap = strm.collect().await;

        data.replace_expire(btreemap);

        let l = Level::new(data, None);
        self.top = Arc::new(l);
    }

    /// Export all its data in RaftStoreEntry format.
    pub async fn export(&self) -> impl Stream<Item = RaftStoreEntry> + '_ {
        let d = self.top.data_ref();

        let mut sm_meta = vec![];

        // Data header to identify snapshot version

        sm_meta.push(RaftStoreEntry::DataHeader {
            key: OnDisk::KEY_HEADER.to_string(),
            value: Header::this_version(),
        });

        // Last applied

        if let Some(last_applied) = d.last_applied_ref() {
            sm_meta.push(RaftStoreEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastApplied,
                value: StateMachineMetaValue::LogId(*last_applied),
            })
        }

        // Last membership

        {
            let last_membership = d.last_membership_ref();
            sm_meta.push(RaftStoreEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastMembership,
                value: StateMachineMetaValue::Membership(last_membership.clone()),
            })
        }

        // Sequence

        sm_meta.push(RaftStoreEntry::Sequences {
            // Use this fixed key `generic-kv` for back compatibility:
            // Only this key is used.
            key: s("generic-kv"),
            value: SeqNum(d.curr_seq()),
        });

        // Nodes

        for (node_id, node) in d.nodes_ref().iter() {
            sm_meta.push(RaftStoreEntry::Nodes {
                key: *node_id,
                value: node.clone(),
            })
        }

        // kv

        let kv_iter = MapApi::<String>::range::<String, _>(self.top.as_ref(), ..)
            .await
            .filter_map(|(k, v)| async move {
                if let Marked::Normal {
                    internal_seq,
                    value,
                    meta,
                } = v
                {
                    let seqv = SeqV::with_meta(*internal_seq, meta.clone(), value.clone());
                    Some(RaftStoreEntry::GenericKV {
                        key: k.clone(),
                        value: seqv,
                    })
                } else {
                    None
                }
            });

        // expire index

        let expire_iter = MapApi::<ExpireKey>::range(self.top.as_ref(), ..)
            .await
            .filter_map(|(k, v)| async move {
                if let Marked::Normal {
                    internal_seq,
                    value,
                    meta: _,
                } = v
                {
                    let ev = ExpireValue::new(value, *internal_seq);

                    Some(RaftStoreEntry::Expire {
                        key: k.clone(),
                        value: ev,
                    })
                } else {
                    None
                }
            });

        futures::stream::iter(sm_meta)
            .chain(kv_iter)
            .chain(expire_iter)
    }
}

fn s(x: impl ToString) -> String {
    x.to_string()
}
