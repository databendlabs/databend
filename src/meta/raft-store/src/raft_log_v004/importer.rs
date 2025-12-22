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
use crate::raft_log_v004::RaftLogV004;
use crate::raft_log_v004::codec_wrapper::Cw;
use crate::raft_log_v004::log_store_meta::LogStoreMeta;
use crate::raft_log_v004::util;

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

            //////////////////////////// V004 log ////////////////////////////
            RaftStoreEntry::LogEntry(log_entry) => {
                let log_id = log_entry.log_id;
                let payload = log_entry.payload;

                self.raft_log.append([(Cw(log_id), Cw(payload))])?;
                self.max_log_id = std::cmp::max(self.max_log_id, Some(log_id));
            }

            RaftStoreEntry::NodeId(node_id) => {
                self.raft_log
                    .save_user_data(Some(LogStoreMeta { node_id }))?;
            }

            RaftStoreEntry::Vote(vote) => {
                if let Some(vote) = vote {
                    self.raft_log.save_vote(Cw(vote))?;
                }
            }

            RaftStoreEntry::Committed(committed) => {
                if let Some(committed) = committed {
                    self.raft_log.commit(Cw(committed))?;
                }
            }

            RaftStoreEntry::Purged(purged) => {
                if let Some(purged) = purged {
                    self.raft_log.purge(Cw(purged))?;
                }
            }

            ///////////////////////// V003 and before Log ////////////////////
            RaftStoreEntry::Logs { .. } => {
                unreachable!("V003 Logs should be written to V004 log");
            }
            RaftStoreEntry::RaftStateKV { .. } => {
                unreachable!("V003 RaftStateKV should be written to V004 log");
            }
            RaftStoreEntry::LogMeta { .. } => {
                unreachable!("V003 LogMeta should be written to V004 log");
            }

            //////////////////////// State machine entries ///////////////////////
            RaftStoreEntry::StateMachineMeta { .. } => {
                unreachable!("StateMachineMeta should be written to log");
            }
            RaftStoreEntry::Nodes { .. } => {
                unreachable!("Nodes should be written to log");
            }
            RaftStoreEntry::Expire { .. } => {
                unreachable!("Expire should be written to log");
            }
            RaftStoreEntry::GenericKV { .. } => {
                unreachable!("GenericKV should be written to log");
            }
            RaftStoreEntry::Sequences { .. } => {
                unreachable!("Sequences should be written to log");
            }
        }

        Ok(())
    }
}
