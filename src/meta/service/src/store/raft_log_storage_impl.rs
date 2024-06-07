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

use std::fmt::Debug;
use std::io::ErrorKind;
use std::ops::RangeBounds;
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::io;
use databend_common_meta_sled_store::openraft::storage::LogFlushed;
use databend_common_meta_sled_store::openraft::storage::RaftLogStorage;
use databend_common_meta_sled_store::openraft::ErrorSubject;
use databend_common_meta_sled_store::openraft::ErrorVerb;
use databend_common_meta_sled_store::openraft::LogIdOptionExt;
use databend_common_meta_sled_store::openraft::LogState;
use databend_common_meta_sled_store::openraft::OptionalSend;
use databend_common_meta_sled_store::openraft::RaftLogReader;
use databend_common_meta_types::Entry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use log::debug;
use log::error;
use log::info;

use crate::metrics::raft_metrics;
use crate::store::RaftStore;
use crate::store::ToStorageError;

impl RaftLogReader<TypeConfig> for RaftStore {
    #[minitrace::trace]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        debug!(
            "RaftStore::try_get_log_entries: self.id={}, range: {:?}",
            self.id, range
        );

        match self
            .log
            .read()
            .await
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Ok(entries) => Ok(entries),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("try_get_log_entries", false);
                Err(err)
            }
        }
    }
}

impl RaftLogStorage<TypeConfig> for RaftStore {
    type LogReader = RaftStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged_log_id = match self
            .log
            .read()
            .await
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last = match self
            .log
            .read()
            .await
            .logs()
            .last()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x.1.log_id),
        };

        debug!(
            "get_log_state: ({:?},{:?}]",
            last_purged_log_id, last_log_id
        );

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.raft_state
            .write()
            .await
            .save_committed(committed)
            .await
            .map_to_sto_err(ErrorSubject::Store, ErrorVerb::Write)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        self.raft_state
            .read()
            .await
            .read_committed()
            .map_to_sto_err(ErrorSubject::Store, ErrorVerb::Read)
    }

    #[minitrace::trace]
    async fn save_vote(&mut self, hs: &Vote) -> Result<(), StorageError> {
        info!(id = self.id; "RaftStore::save_vote({}): start", hs);

        let res = self
            .raft_state
            .write()
            .await
            .save_vote(hs)
            .await
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Write);

        info!(id = self.id; "RaftStore::save_vote({}): done", hs);

        match res {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("save_vote", true);
                Err(err)
            }
            Ok(_) => Ok(()),
        }
    }

    #[minitrace::trace]
    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        match self
            .raft_state
            .read()
            .await
            .read_vote()
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("read_vote", false);
                Err(err)
            }
            Ok(vote) => Ok(vote),
        }
    }

    #[minitrace::trace]
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut first = None;
        let mut last = None;

        let entries = entries
            .into_iter()
            .map(|x| {
                if first.is_none() {
                    first = Some(x.log_id);
                }
                last = Some(x.log_id);
                x
            })
            .collect::<Vec<_>>();

        info!("RaftStore::append([{:?}, {:?}]): start", first, last);

        let res = match self.log.write().await.append(entries).await {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("append_to_log", true);
                Err(err)
            }
            Ok(_) => Ok(()),
        };

        callback.log_io_completed(res.map_err(|e| io::Error::new(ErrorKind::InvalidData, e)));

        info!("RaftStore::append([{:?}, {:?}]): done", first, last);

        Ok(())
    }

    #[minitrace::trace]
    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!(id = self.id; "RaftStore::truncate({}): start", log_id);

        let res = self
            .log
            .write()
            .await
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete);

        info!(id = self.id; "RaftStore::truncate({}): done", log_id);

        match res {
            Ok(_) => Ok(()),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("delete_conflict_logs_since", true);
                Err(err)
            }
        }
    }

    #[minitrace::trace]
    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let curr_purged = self
            .log
            .write()
            .await
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;

        let purge_range = (curr_purged.next_index(), log_id.index);
        let purge_range_str = format!("({},{}]", purge_range.0, purge_range.1);

        info!(
            id = self.id,
            curr_purged :? =(&curr_purged),
            upto_log_id :? =(&log_id);
            "RaftStore::purge({}): start", purge_range_str);

        if let Err(err) = self
            .log
            .write()
            .await
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        };

        info!(id = self.id, log_id :? =(&log_id); "RaftStore::purge({}): Done: set_last_purged()", purge_range_str);

        let log = self.log.write().await.clone();

        // Purge can be done in another task safely, because:
        //
        // - Next time when raft starts, it will read last_purged_log_id without examining the actual first log.
        //   And junk can be removed next time purge_logs_upto() is called.
        //
        // - Purging operates the start of the logs, and only committed logs are purged;
        //   while append and truncate operates on the end of the logs,
        //   it is safe to run purge && (append || truncate) concurrently.
        databend_common_base::runtime::spawn({
            let id = self.id;
            async move {
                info!(id = id, log_id :? =(&log_id); "RaftStore::purge({}): Start: asynchronous one by one remove", purge_range_str);

                let mut removed_cnt = 0;
                let mut removed_size = 0;
                let curr = curr_purged.next_index();
                for i in curr..=log_id.index {
                    let res = log.logs().remove_no_return(&i, true).await;

                    let removed = match res {
                        Ok(r) => r,
                        Err(err) => {
                            error!(id = id, log_index :% =i;
                                "RaftStore::purge({}): in asynchronous error: {}", purge_range_str, err);
                            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
                            return;
                        }
                    };

                    if let Some(size) = removed {
                        removed_cnt += 1;
                        removed_size += size;
                    } else {
                        error!(id = id, log_index :% =i;
                            "RaftStore::purge({}): in asynchronous error: not found, maybe removed by other thread; quit this thread", purge_range_str);
                        return;
                    }

                    if i % 100 == 0 {
                        info!(id = id, log_index :% =i,
                            removed_cnt = removed_cnt,
                            removed_size = removed_size,
                            avg_removed_size = removed_size / (removed_cnt+1);
                            "RaftStore::purge({}): asynchronous removed log", purge_range_str);
                    }

                    // Do not block for too long if there are many keys to delete.
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }

                info!(id = id, upto_log_id :? =(&log_id); "RaftStore::purge({}): Done: asynchronous one by one remove", purge_range_str);
            }
        });

        Ok(())
    }
}
