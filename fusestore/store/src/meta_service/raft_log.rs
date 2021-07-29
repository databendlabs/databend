// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Deref;
use std::ops::RangeBounds;

use async_raft::raft::Entry;
use common_tracing::tracing;

use crate::meta_service::LogEntry;
use crate::meta_service::LogIndex;
use crate::meta_service::SledSerde;
use crate::meta_service::SledTree;
use crate::meta_service::SledValueToKey;

const TREE_RAFT_LOG: &str = "raft_log";

/// RaftLog stores the logs of a raft node.
/// It is part of MetaStore.
pub struct RaftLog {
    inner: SledTree<LogIndex, Entry<LogEntry>>,
}

/// Allows to access directly the internal SledTree.
impl Deref for RaftLog {
    type Target = SledTree<LogIndex, Entry<LogEntry>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl SledSerde for Entry<LogEntry> {}

impl SledValueToKey<LogIndex> for Entry<LogEntry> {
    fn to_key(&self) -> LogIndex {
        self.log_id.index
    }
}

impl RaftLog {
    /// Open RaftLog
    pub async fn open(db: &sled::Db) -> common_exception::Result<RaftLog> {
        let rl = RaftLog {
            inner: SledTree::open(db, TREE_RAFT_LOG).await?,
        };
        Ok(rl)
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
    ///
    pub async fn range_delete<R>(&self, range: R) -> common_exception::Result<()>
    where R: RangeBounds<LogIndex> {
        self.inner.range_delete(range, true).await
    }

    /// Append logs into RaftLog.
    /// There is no consecutiveness check. It is the caller's responsibility to leave no holes(if it runs a standard raft:DDD).
    /// There is no overriding check either. It always overrides the existent ones.
    ///
    /// When this function returns the logs are guaranteed to be fsync-ed.
    pub async fn append(&self, logs: &[Entry<LogEntry>]) -> common_exception::Result<()> {
        self.inner.append_values(logs).await
    }

    /// Insert a single log.
    #[tracing::instrument(level = "debug", skip(self, log), fields(log_id=format!("{}",log.log_id).as_str()))]
    pub async fn insert(
        &self,
        log: &Entry<LogEntry>,
    ) -> common_exception::Result<Option<Entry<LogEntry>>> {
        self.inner.insert_value(log).await
    }
}
