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

use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::sys_data::SysData;
use rotbl::v001::SeqMarked;
use state_machine_api::UserKey;

use crate::key_spaces::SMEntry;
use crate::leveled_store::rotbl_codec::RotblCodec;
use crate::state_machine::StateMachineMetaKey;

/// Convert V002 snapshot lines in json of [`SMEntry`]
/// to V004 rotbl key-value pairs. `(String, SeqMarked)`,
/// or update SysData in place.
///
/// It holds a lock of SysData because this converter may be run concurrently in multiple threads.
pub struct SMEntryV002ToV004 {
    pub sys_data: Arc<Mutex<SysData>>,
}

impl SMEntryV002ToV004 {
    pub fn convert_line(&mut self, s: &str) -> Result<Option<(String, SeqMarked)>, io::Error> {
        let ent: SMEntry =
            serde_json::from_str(s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let kv = self.sm_entry_to_rotbl_kv(ent)?;
        Ok(kv)
    }

    pub fn sm_entry_to_rotbl_kv(
        &mut self,
        ent: SMEntry,
    ) -> Result<Option<(String, SeqMarked)>, io::Error> {
        match ent {
            SMEntry::DataHeader { .. } => {
                // Snapshot V004 in rotbl format does not store data header
            }
            SMEntry::Nodes { key, value } => {
                let mut s = self.sys_data.lock().unwrap();
                s.nodes_mut().insert(key, value);
            }
            SMEntry::StateMachineMeta { key, value } => {
                let mut s = self.sys_data.lock().unwrap();
                match key {
                    StateMachineMetaKey::LastApplied => {
                        // Infallible
                        let last_applied: LogId = value.try_into().unwrap();
                        *s.last_applied_mut() = Some(last_applied);
                    }
                    StateMachineMetaKey::Initialized => {
                        // Obsolete
                    }
                    StateMachineMetaKey::LastMembership => {
                        // Infallible
                        let last_membership: StoredMembership = value.try_into().unwrap();
                        *s.last_membership_mut() = last_membership;
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

                let marked = SeqMarked::from(value);
                let (k, v) = RotblCodec::encode_key_seq_marked(&key, marked)?;
                return Ok(Some((k, v)));
            }
            SMEntry::GenericKV { key, value } => {
                let marked = SeqMarked::from(value);
                let (k, v) =
                    RotblCodec::encode_key_seq_marked(UserKey::from_string_ref(&key), marked)?;
                return Ok(Some((k, v)));
            }
            SMEntry::Sequences { key: _, value } => {
                let mut s = self.sys_data.lock().unwrap();
                s.update_seq(value.0);
            }
        }
        Ok(None)
    }
}
