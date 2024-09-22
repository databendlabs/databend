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

use std::ops::RangeBounds;

use databend_common_meta_sled_store::sled;
use databend_common_meta_sled_store::AsKeySpace;
use databend_common_meta_sled_store::SledTree;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::Entry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::LogIndex;
use log::info;

use crate::config::RaftConfig;
use crate::key_spaces::LogMeta;
use crate::key_spaces::Logs;
use crate::state_machine::LogMetaKey;
use crate::state_machine::LogMetaValue;

pub const TREE_RAFT_LOG: &str = "raft_log";

/// RaftLog stores the logs of a raft node.
/// It is part of MetaStore.
#[derive(Clone)]
pub struct RaftLog {
    pub inner: SledTree,
}

impl RaftLog {
    /// Open RaftLog
    #[fastrace::trace]
    pub async fn open(db: &sled::Db, config: &RaftConfig) -> Result<RaftLog, MetaStorageError> {
        info!(config :? =(config); "open RaftLog");

        let tree_name = config.tree_name(TREE_RAFT_LOG);
        let inner = SledTree::open(db, tree_name, config.is_sync())?;
        let rl = RaftLog { inner };
        Ok(rl)
    }

    pub async fn set_last_purged(&self, log_id: LogId) -> Result<(), MetaStorageError> {
        self.log_meta()
            .insert(&LogMetaKey::LastPurged, &LogMetaValue::LogId(log_id))
            .await?;
        Ok(())
    }

    pub fn get_last_purged(&self) -> Result<Option<LogId>, MetaStorageError> {
        let res = self.log_meta().get(&LogMetaKey::LastPurged)?;
        match res {
            None => Ok(None),
            Some(l) => {
                let log_id: LogId = l.try_into().unwrap();
                Ok(Some(log_id))
            }
        }
    }

    /// Delete logs that are in `range`.
    ///
    /// When this function returns the logs are guaranteed to be fsync-ed.
    ///
    /// TODO(xp): in raft deleting logs may not need to be fsync-ed.
    ///
    /// 1. Deleting happens when cleaning applied logs, in which case, these logs will never be read:
    ///    The logs to clean are all included in a snapshot and state machine.
    ///    Replication will use the snapshot for sync, or create a new snapshot from the state machine for sync.
    ///    Thus these logs will never be read. If an un-fsync-ed delete is lost during server crash, it just wait for next delete to clean them up.
    ///
    /// 2. Overriding uncommitted logs of an old term by some new leader that did not see these logs:
    ///    In this case, atomic delete is quite enough(to not leave a hole).
    ///    If the system allows logs hole, non-atomic delete is quite enough(depends on the upper layer).
    pub async fn range_remove<R>(&self, range: R) -> Result<(), MetaStorageError>
    where R: RangeBounds<LogIndex> {
        self.logs().range_remove(range, true).await
    }

    pub fn range_values<R>(&self, range: R) -> Result<Vec<Entry>, MetaStorageError>
    where R: RangeBounds<LogIndex> {
        self.logs().range_values(range)
    }

    /// Append logs into RaftLog.
    /// There is no consecutive-ness check. It is the caller's responsibility to leave no holes(if it runs a standard raft:DDD).
    /// There is no overriding check either. It always overrides the existent ones.
    ///
    /// When this function returns the logs are guaranteed to be fsync-ed.
    pub async fn append<I: IntoIterator<Item = Entry>>(
        &self,
        logs: I,
    ) -> Result<(), MetaStorageError> {
        self.logs().append(logs).await
    }

    /// Returns a borrowed key space in sled::Tree for logs
    pub fn logs(&self) -> AsKeySpace<Logs> {
        self.inner.key_space()
    }

    /// Returns a borrowed key space in sled::Tree for logs
    pub fn log_meta(&self) -> AsKeySpace<LogMeta> {
        self.inner.key_space()
    }
}
