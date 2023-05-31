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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_meta_stoerr::MetaBytesError;
use common_meta_types::LogId;
use common_meta_types::SeqNum;
use common_meta_types::SeqV;
use common_meta_types::StoredMembership;
use openraft::AnyError;

use crate::key_spaces::RaftStoreEntry;
use crate::sm2::leveled_store::level::Level;
use crate::sm2::leveled_store::level_data::LevelData;
use crate::sm2::leveled_store::map_api::MapApi;
use crate::sm2::marked::Marked;
use crate::state_machine::ExpireKey;
use crate::state_machine::ExpireValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

pub struct Snapshot {
    top: Arc<Level>,
    original: Arc<Level>,
}

impl Snapshot {
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

    /// Compact and return a Level with all tombstone removed.
    pub fn compact(&mut self) {
        // TODO: use a explicit method to return a compaction base
        let mut data = self.top.data_ref().new_level();

        let it = MapApi::<String>::range::<String, _>(self.top.as_ref(), ..)
            .filter(|(_k, v)| !v.is_tomb_stone());

        data.replace_kv(it.map(|(k, v)| (k.clone(), v.clone())).collect());

        let it =
            MapApi::<ExpireKey>::range(self.top.as_ref(), ..).filter(|(_k, v)| !v.is_tomb_stone());

        data.replace_expire(it.map(|(k, v)| (k.clone(), v.clone())).collect());

        let l = Level::new(data, None);
        self.top = Arc::new(l);
    }

    pub fn export(&self) -> impl Iterator<Item = RaftStoreEntry> + '_ {
        let d = self.top.data_ref();

        // Sequence

        let mut sm_meta = vec![RaftStoreEntry::Sequences {
            // For back compatibility.
            key: s("generic-kv"),
            value: SeqNum(d.curr_seq()),
        }];

        // Last applied

        if let Some(last_applied) = d.last_applied_ref() {
            sm_meta.push(RaftStoreEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastApplied,
                value: StateMachineMetaValue::LogId(last_applied.clone()),
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

        // Nodes

        for (node_id, node) in d.nodes_ref().iter() {
            sm_meta.push(RaftStoreEntry::Nodes {
                key: *node_id,
                value: node.clone(),
            })
        }

        // kv

        let kv_iter =
            MapApi::<String>::range::<String, _>(self.top.as_ref(), ..).filter_map(|(k, v)| {
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

        let expire_iter = MapApi::<ExpireKey>::range(self.top.as_ref(), ..).filter_map(|(k, v)| {
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

        sm_meta.into_iter().chain(kv_iter).chain(expire_iter)
    }

    pub fn new_importer() -> Importer {
        Importer::default()
    }

    pub fn import(data: impl Iterator<Item = RaftStoreEntry>) -> Result<LevelData, MetaBytesError> {
        let mut importer = Self::new_importer();

        for ent in data {
            importer.import(ent)?;
        }

        Ok(importer.commit())
    }
}

/// A container of temp data that are imported to a LevelData.
#[derive(Debug, Default)]
pub struct Importer {
    level_data: LevelData,

    kv: BTreeMap<String, Marked>,
    expire: BTreeMap<ExpireKey, Marked<String>>,

    greatest_seq: u64,
}

impl Importer {
    // TODO(1): consider returning IO error
    pub fn import(&mut self, entry: RaftStoreEntry) -> Result<(), MetaBytesError> {
        let d = &mut self.level_data;

        match entry {
            RaftStoreEntry::DataHeader { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::Logs { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::LogMeta { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::RaftStateKV { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::ClientLastResps { .. } => {
                unreachable!("client last resp is not supported")
            }
            RaftStoreEntry::Nodes { key, value } => {
                d.nodes_mut().insert(key, value);
            }
            RaftStoreEntry::StateMachineMeta { key, value } => {
                match key {
                    StateMachineMetaKey::LastApplied => {
                        let lid = TryInto::<LogId>::try_into(value).map_err(|e| {
                            MetaBytesError::new(&AnyError::error(format_args!(
                                "{} when import StateMachineMetaKey::LastApplied",
                                e
                            )))
                        })?;

                        *d.last_applied_mut() = Some(lid);
                    }
                    StateMachineMetaKey::Initialized => {
                        // This field is no longer used by in-memory state machine
                    }
                    StateMachineMetaKey::LastMembership => {
                        let membership =
                            TryInto::<StoredMembership>::try_into(value).map_err(|e| {
                                MetaBytesError::new(&AnyError::error(format_args!(
                                    "{} when import StateMachineMetaKey::LastMembership",
                                    e
                                )))
                            })?;
                        *d.last_membership_mut() = membership;
                    }
                }
            }
            RaftStoreEntry::Expire { key, mut value } => {
                // Old version ExpireValue has seq to be 0. replace it with 1.
                // `1` is a valid seq. `0` is used by tombstone.
                // 2023-06-06: by drdr.xp@gmail.com
                if value.seq == 0 {
                    value.seq = 1;
                }

                self.greatest_seq = std::cmp::max(self.greatest_seq, value.seq);
                self.expire.insert(key, Marked::from(value));
            }
            RaftStoreEntry::GenericKV { key, value } => {
                self.greatest_seq = std::cmp::max(self.greatest_seq, value.seq);
                self.kv.insert(key, Marked::from(value));
            }
            RaftStoreEntry::Sequences { key: _, value } => d.update_seq(value.0),
        }

        Ok(())
    }

    pub fn commit(mut self) -> LevelData {
        let d = &mut self.level_data;

        d.replace_kv(self.kv);
        d.replace_expire(self.expire);

        assert!(
            self.greatest_seq <= d.curr_seq(),
            "greatest_seq {} must be LE curr_seq {}, otherwise seq may be reused",
            self.greatest_seq,
            d.curr_seq()
        );

        self.level_data
    }
}

fn s(x: impl ToString) -> String {
    x.to_string()
}
