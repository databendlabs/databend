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
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;

use databend_common_meta_raft_store::raft_log_v004;
use databend_common_meta_raft_store::raft_log_v004::codec_wrapper::Cw;
use databend_common_meta_raft_store::raft_log_v004::io_desc::IODesc;
use databend_common_meta_sled_store::openraft::EntryPayload;
use databend_common_meta_sled_store::openraft::LogIdOptionExt;
use databend_common_meta_sled_store::openraft::LogState;
use databend_common_meta_sled_store::openraft::OptionalSend;
use databend_common_meta_sled_store::openraft::RaftLogReader;
use databend_common_meta_sled_store::openraft::entry::RaftEntry;
use databend_common_meta_sled_store::openraft::storage::RaftLogStorage;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::IOFlushed;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::Membership;
use databend_common_meta_types::raft_types::TypeConfig;
use databend_common_meta_types::raft_types::Vote;
use deepsize::DeepSizeOf;
use display_more::DisplayOptionExt;
use itertools::Itertools;
use log::debug;
use log::info;
use log::warn;
use raft_log::api::raft_log_writer::RaftLogWriter;
use tokio::sync::oneshot;

use crate::store::meta_raft_log::MetaRaftLog;

impl RaftLogReader<TypeConfig> for MetaRaftLog {
    #[fastrace::trace]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let (start, end) = range_boundary(range);

        let mut io = IODesc::read_logs(format!(
            "RaftStore(id={})::try_get_log_entries([{},{})",
            self.id, start, end
        ));

        let log = self.read().await;

        let entries = log
            .read(start, end)
            .map_ok(|(log_id, payload)| Entry {
                log_id: log_id.0,
                payload: payload.0,
            })
            .collect::<Result<Vec<_>, _>>()?;

        io.set_done_time();
        info!("{}", io.ok_done());
        Ok(entries)
    }

    #[fastrace::trace]
    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        let log = self.read().await;
        let vote = log.log_state().vote().map(Cw::to_inner);

        Ok(vote)
    }

    #[fastrace::trace]
    async fn limited_get_log_entries(
        &mut self,
        mut start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, io::Error> {
        let chunk_size = 8;
        let max_size = 2 * 1024 * 1024;

        let mut res = Vec::with_capacity(64);
        let mut total_size = 0;

        while start < end {
            let chunk_end = std::cmp::min(start + chunk_size, end);
            let entries = self.try_get_log_entries(start..chunk_end).await?;

            for ent in entries {
                let size = match &ent.payload {
                    EntryPayload::Blank => 0,
                    EntryPayload::Normal(log_entry) => log_entry.deep_size_of(),
                    EntryPayload::Membership(_) => size_of::<Membership>(),
                };

                debug!(
                    "RaftStore::limited_get_log_entries: got log: log_id: {}, size: {}",
                    ent.log_id(),
                    size
                );

                res.push(ent);
                total_size += size;

                if total_size >= max_size {
                    info!(
                        "RaftStore::limited_get_log_entries: too many logs, early return: entries cnt: {}, total size: {}, res: [{}, {}]",
                        res.len(),
                        total_size,
                        res.first().map(|x| x.log_id()).unwrap(),
                        res.last().map(|x| x.log_id()).unwrap(),
                    );

                    return Ok(res);
                }
            }

            start = chunk_end;
        }

        Ok(res)
    }
}

impl RaftLogStorage<TypeConfig> for MetaRaftLog {
    type LogReader = MetaRaftLog;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let log = self.read().await;
        let state = log.log_state();

        let purged = state.purged().map(Cw::to_inner);
        let last = state.last().map(Cw::to_inner);

        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[fastrace::trace]
    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let io = IODesc::save_vote(format!("RaftStore(id={})::save_vote({})", self.id, vote));

        let (tx, rx) = oneshot::channel();

        {
            let mut log = self.write().await;

            log.save_vote(Cw(*vote))?;
            log.flush(raft_log_v004::Callback::new_oneshot(tx, io.clone()))?;
        }

        rx.await.map_err(io::Error::other)??;

        info!("{}: done", io.ok_done());
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        let io = IODesc::save_committed(format!(
            "RaftStore(id={})::save_committed({})",
            self.id,
            committed.display()
        ));

        let Some(committed) = committed else {
            warn!("{}: skip save_committed(None)", io);
            return Ok(());
        };

        {
            let mut log = self.write().await;
            log.commit(Cw(committed))?;
        }

        info!(
            "{}; No need to flush committed, reversion is acceptable",
            io.ok_submit()
        );
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        let log = self.read().await;
        let committed = log.log_state().committed().map(Cw::to_inner);

        Ok(committed)
    }

    #[fastrace::trace]
    async fn append<I>(&mut self, entries: I, callback: IOFlushed) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut entries = entries
            .into_iter()
            .map(|x| (Cw(x.log_id), Cw(x.payload)))
            .peekable();

        let first = entries.peek().map(|x| x.0);

        let io = IODesc::append(format!(
            "RaftStore(id={})::append([{}, ...])",
            self.id,
            first.display()
        ));

        let mut log = self.write().await;

        log.append(entries)?;

        debug!("{}", io.ok_submit());

        log.flush(raft_log_v004::Callback::new_io_flushed(
            callback,
            io.clone(),
        ))?;

        info!("{}", io.ok_submit_flush());

        Ok(())
    }

    #[fastrace::trace]
    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        // truncate_after(None) means delete all entries (keep nothing)
        // truncate_after(Some(log_id)) means keep entries up to and including log_id
        let truncate_at = last_log_id.next_index();

        let io = IODesc::truncate(format!(
            "RaftStore(id={})::truncate_after({:?})",
            self.id, last_log_id
        ));

        let mut log = self.write().await;

        {
            let curr_last = log.log_state().last().map(Cw::to_inner);
            if truncate_at >= curr_last.next_index() {
                warn!(
                    "{}: truncate_at({}) >= curr_last.next_index({}), skip truncate",
                    io,
                    truncate_at,
                    curr_last.next_index()
                );
                return Ok(());
            }
        }

        log.truncate(truncate_at)?;

        // No need to flush a truncate operation.
        info!("{}; No need to flush", io.ok_submit());
        Ok(())
    }

    #[fastrace::trace]
    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let io = IODesc::purge(format!("RaftStore(id={})::purge(upto={})", self.id, log_id));

        let mut log = self.write().await;

        {
            let curr_purged = log.log_state().purged().map(Cw::to_inner);
            if log_id.index < curr_purged.next_index() {
                warn!(
                    "{}: before curr_purged({}), skip purge",
                    io,
                    curr_purged.display()
                );
                return Ok(());
            }
        }

        log.purge(Cw(log_id))?;

        info!("{}; No need to flush", io.ok_submit());
        Ok(())
    }
}

fn range_boundary<RB: RangeBounds<u64>>(range: RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };

    let end = match range.end_bound() {
        Bound::Included(&n) => n + 1,
        Bound::Excluded(&n) => n,
        Bound::Unbounded => u64::MAX,
    };

    (start, end)
}
