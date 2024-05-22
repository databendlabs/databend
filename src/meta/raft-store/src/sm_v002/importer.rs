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
use std::io;

use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::LogId;
use databend_common_meta_types::StoredMembership;

use crate::key_spaces::SMEntry;
use crate::leveled_store::level::Level;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::marked::Marked;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineMetaKey;

/// A container of temp data that are imported to a LevelData.
#[derive(Debug, Default)]
pub struct Importer {
    level_data: Level,

    kv: BTreeMap<String, Marked>,
    expire: BTreeMap<ExpireKey, Marked<String>>,

    greatest_seq: u64,
}

impl Importer {
    pub fn import(&mut self, entry: SMEntry) -> Result<(), io::Error> {
        let d = &mut self.level_data;

        match entry {
            SMEntry::DataHeader { .. } => {
                // Not part of state machine
            }
            SMEntry::Nodes { key, value } => {
                d.sys_data_mut().nodes_mut().insert(key, value);
            }
            SMEntry::StateMachineMeta { key, value } => {
                match key {
                    StateMachineMetaKey::LastApplied => {
                        let lid = TryInto::<LogId>::try_into(value).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                AnyError::error(e)
                                    .add_context(|| "import StateMachineMetaKey::LastApplied"),
                            )
                        })?;

                        *d.sys_data_mut().last_applied_mut() = Some(lid);
                    }
                    StateMachineMetaKey::Initialized => {
                        // This field is no longer used by in-memory state machine
                    }
                    StateMachineMetaKey::LastMembership => {
                        let membership =
                            TryInto::<StoredMembership>::try_into(value).map_err(|e| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    AnyError::error(e).add_context(
                                        || "import StateMachineMetaKey::LastMembership",
                                    ),
                                )
                            })?;
                        *d.sys_data_mut().last_membership_mut() = membership;
                    }
                }
            }
            SMEntry::Expire { key, mut value } => {
                // Old version ExpireValue has seq to be 0. replace it with 1.
                // `1` is a valid seq. `0` is used by tombstone.
                // 2023-06-06: by drdr.xp@gmail.com
                if value.seq == 0 {
                    value.seq = 1;
                }

                self.greatest_seq = std::cmp::max(self.greatest_seq, value.seq);
                self.expire.insert(key, Marked::from(value));
            }
            SMEntry::GenericKV { key, value } => {
                self.greatest_seq = std::cmp::max(self.greatest_seq, value.seq);
                self.kv.insert(key, Marked::from(value));
            }
            SMEntry::Sequences { key: _, value } => d.sys_data_mut().update_seq(value.0),
        }

        Ok(())
    }

    pub fn commit(mut self) -> Level {
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
