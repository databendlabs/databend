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

use databend_common_meta_types::raft_types::LogId;
use raft_log::api::raft_log_writer::RaftLogWriter;

use crate::key_spaces::RaftStoreEntry;
use crate::raft_log_v004::codec_wrapper::Cw;
use crate::raft_log_v004::log_store_meta::LogStoreMeta;
use crate::raft_log_v004::util;
use crate::raft_log_v004::RaftLogV004;
use crate::state::RaftStateKey;
use crate::state_machine::LogMetaKey;

/// Import series of [`RaftStoreEntry`] record into [`RaftLogV004`].
///
/// [`RaftStoreEntry`] is line-wise format for export or data backup.
pub struct Importer {
    pub raft_log: RaftLogV004,
    pub max_log_id: Option<LogId>,
}

impl Importer {
    pub fn new(raft_log: RaftLogV004) -> Self {
        Importer {
            raft_log,
            max_log_id: None,
        }
    }

    pub async fn flush(mut self) -> Result<RaftLogV004, io::Error> {
        util::blocking_flush(&mut self.raft_log).await?;
        Ok(self.raft_log)
    }

    pub fn import_raft_store_entry(&mut self, entry: RaftStoreEntry) -> Result<(), io::Error> {
        match entry {
            RaftStoreEntry::DataHeader { .. } => {
                // V004 RaftLog does not store DataHeader
            }
            RaftStoreEntry::Logs { key: _, value } => {
                let (log_id, payload) = (value.log_id, value.payload);

                self.raft_log.append([(Cw(log_id), Cw(payload))])?;
                self.max_log_id = std::cmp::max(self.max_log_id, Some(value.log_id));
            }
            RaftStoreEntry::RaftStateKV { key, value } => match key {
                RaftStateKey::Id => {
                    self.raft_log.save_user_data(Some(LogStoreMeta {
                        node_id: Some(value.node_id()),
                    }))?;
                }
                RaftStateKey::HardState => {
                    self.raft_log.save_vote(Cw(value.vote()))?;
                }
                RaftStateKey::StateMachineId => {
                    unreachable!("StateMachineId is removed");
                }
                RaftStateKey::Committed => {
                    if let Some(value) = value.committed() {
                        self.raft_log.commit(Cw(value))?;
                    }
                }
            },
            RaftStoreEntry::LogMeta { key, value } => match key {
                LogMetaKey::LastPurged => {
                    let purged = value.log_id();
                    self.raft_log.purge(Cw(purged))?;
                }
            },

            //////////////////////////////////////////////////////////////////
            RaftStoreEntry::StateMachineMeta { .. } => {
                unreachable!("StateMachineMeta should be written to snapshot");
            }
            RaftStoreEntry::Nodes { .. } => {
                unreachable!("Nodes should be written to snapshot");
            }
            RaftStoreEntry::Expire { .. } => {
                unreachable!("Expire should be written to snapshot");
            }
            RaftStoreEntry::GenericKV { .. } => {
                unreachable!("GenericKV should be written to snapshot");
            }
            RaftStoreEntry::Sequences { .. } => {
                unreachable!("Sequences should be written to snapshot");
            }
            RaftStoreEntry::ClientLastResps { .. } => {
                unreachable!("ClientLastResps should be written to snapshot");
            }
        }

        Ok(())
    }
}
