// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::RangeBounds;

use async_raft::raft::Entry;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::sled_serde::SledRangeSerde;
use crate::meta_service::LogEntry;
use crate::meta_service::SledSerde;

const K_RAFT_LOG: &str = "raft_log";

/// RaftLog stores the logs of a raft node.
/// It is part of MetaStore.
pub struct RaftLog {
    tree: sled::Tree,
}

impl SledSerde for Entry<LogEntry> {}

impl RaftLog {
    /// Open RaftLog
    pub async fn open(db: &sled::Db) -> common_exception::Result<RaftLog> {
        let t = db
            .open_tree(K_RAFT_LOG)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "open tree raft_log")?;

        let rl = RaftLog { tree: t };
        Ok(rl)
    }

    /// Retrieve the last log index and the log entry.
    pub fn last(&self) -> common_exception::Result<Option<(u64, Entry<LogEntry>)>> {
        //  TODO(xp): rename LogEntry: Entry<LogEntry> is weird.
        let last = self
            .tree
            .last()
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "read last log")?;

        match last {
            Some((k, v)) => {
                let log_index = u64::de(&k)?;
                let ent = Entry::<LogEntry>::de(&v)?;
                Ok(Some((log_index, ent)))
            }
            None => Ok(None),
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
    ///
    pub async fn range_delete<R>(&self, range: R) -> common_exception::Result<()>
    where R: RangeBounds<u64> {
        let mut batch = sled::Batch::default();

        // Convert u64 range into sled::IVec range
        let range = range.ser()?;

        for item in self.tree.range(range) {
            let (k, _) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || "range_delete")?;
            batch.remove(k);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "batch log delete")?;

        self.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush log delete")?;

        Ok(())
    }

    /// Get logs of index in `range`
    pub fn range_get<R>(&self, range: R) -> common_exception::Result<Vec<Entry<LogEntry>>>
    where R: RangeBounds<u64> {
        // TODO(xp): pre alloc vec space
        let mut res = vec![];

        // Convert u64 range into sled::IVec range
        let range = range.ser()?;
        for item in self.tree.range(range) {
            let (_, v) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || "range_get")?;

            let ent = Entry::<LogEntry>::de(&v)?;
            res.push(ent);
        }

        Ok(res)
    }

    /// Append logs into RaftLog.
    /// There is no consecutiveness check. It is the caller's responsibility to leave no holes(if it runs a standard raft:DDD).
    /// There is no overriding check either. It always overrides the existent ones.
    ///
    /// When this function returns the logs are guaranteed to be fsync-ed.
    pub async fn append(&self, logs: &[Entry<LogEntry>]) -> common_exception::Result<()> {
        let mut batch = sled::Batch::default();

        for log in logs.iter() {
            let index = log.log_id.index;

            let k = index.ser()?;
            let v = log.ser()?;
            // let v = SledSerde::ser(log)?;

            batch.insert(k, v);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "batch log insert")?;

        self.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush log insert")?;

        Ok(())
    }
}
