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
use std::io::Error;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;

use compactor::Compactor;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Node;
use futures_util::StreamExt;
use log::info;
use log::warn;
use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::util;
use map_api::IOResultStream;
use map_api::MapKey;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::applier_acquirer::WriterPermit;
use crate::leveled_store::leveled_map::compacting_data::CompactingData;
use crate::leveled_store::leveled_map::immutable_data::ImmutableData;
use crate::leveled_store::leveled_map::leveled_map_data::LeveledMapData;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::snapshot::MvccSnapshot;
use crate::leveled_store::snapshot::StateMachineSnapshot;
use crate::leveled_store::value_convert::ValueConvert;
use crate::leveled_store::view::StateMachineView;

#[cfg(test)]
mod acquire_compactor_test;

pub mod applier_acquirer;
pub mod compacting_data;
pub mod compactor;
pub mod compactor_acquirer;
pub mod immutable_data;
mod impl_commit;
mod impl_snapshot_get;
mod impl_snapshot_range;
pub mod leveled_map_data;

#[cfg(test)]
mod leveled_map_test;

/// Similar to leveldb.
///
/// The top level is the newest and writable.
/// Others are immutable.
///
/// - A writer must acquire a permit to write_semaphore.
/// - A compactor must:
///   - acquire the compaction_semaphore first,
///   - then acquire `write_semaphore` to move `writeable` to `immutable_levels`,
///
/// The top level is the newest and writable and there is **at most one** candidate writer.
///
/// |                  | writer_semaphore | compactor_semaphore |
/// | :--              | :--              | :--                 |
/// | writable         | RW               |                     |
/// | immutable_levels | R                | RW                  |
/// | persisted        | R                | RW                  |
#[derive(Debug, Clone)]
pub struct LeveledMap {
    pub data: Arc<Mutex<LeveledMapData>>,
}

impl Default for LeveledMap {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(LeveledMapData::default())),
        }
    }
}

// TODO: test it
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Level: GetTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedGet<K, K::V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        let immutable = {
            let inner = self.data.lock().unwrap();
            let got = inner
                .writable
                .get_table()
                .get(key.clone(), snapshot_seq)
                .cloned();
            if !got.is_not_found() {
                return Ok(got);
            }

            inner.immutable.clone()
        };

        immutable.get(key, snapshot_seq).await
    }
}

// TODO: test it
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedRange<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Level: GetTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedRange<K, K::V>,
{
    async fn range<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let mut kmerge = KMerge::by(util::by_key_seq);

        // writable level

        let (vec, immutable) = {
            let inner = self.data.lock().unwrap();
            let it = inner
                .writable
                .get_table()
                .range(range.clone(), snapshot_seq);
            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();

            (vec, inner.immutable.clone())
        };

        if vec.len() > 1000 {
            warn!(
                "Level.writable::range(start={:?}, end={:?}) returns big range of len={}",
                range.start_bound(),
                range.end_bound(),
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        kmerge = kmerge.merge(strm);

        let strm = immutable.range(range, snapshot_seq).await?;
        kmerge = kmerge.merge(strm);

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }
}

impl LeveledMap {
    pub(crate) fn to_view(&self) -> StateMachineView {
        StateMachineView::from_leveled_map(self)
    }

    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.with_inner(|inner| inner.writable.with_sys_data(f))
    }

    /// Freeze the current writable level and create a new empty writable level.
    ///
    /// Need writer permit and compactor permit
    pub fn freeze_writable(&self, _writer_permit: &mut WriterPermit) {
        self.do_freeze_writable()
    }

    /// For testing, requires no permit
    pub fn testing_freeze_writable(&self) {
        self.do_freeze_writable()
    }

    pub fn do_freeze_writable(&self) {
        let mut inner = self.data.lock().unwrap();

        let new_writable = inner.writable.new_level();
        let new_immutable = std::mem::replace(&mut inner.writable, new_writable);

        let mut levels = inner.immutable.levels().clone();
        levels.insert(Immutable::new_from_level(new_immutable));

        let persisted = inner.immutable.persisted().cloned();
        let new_immutable_data = ImmutableData::new(levels.clone(), persisted);
        inner.immutable = Arc::new(new_immutable_data);

        info!(
            "do_freeze_writable: after writable: {:?}, immutables: {:?}",
            inner.writable,
            levels.indexes()
        );
    }

    pub fn persisted(&self) -> Option<DB> {
        self.with_inner(|inner| inner.immutable.persisted().cloned())
    }

    /// Return a reference to the immutable levels.
    pub fn immutable_levels(&self) -> ImmutableLevels {
        self.with_inner(|inner| inner.immutable.levels().clone())
    }

    /// Create a snapshot as a repeatable read readonly view.
    pub(crate) fn to_snapshot(&self) -> MvccSnapshot {
        let seq = self.curr_seq();
        mvcc::Snapshot::new(InternalSeq::new(seq), self.clone())
    }

    pub(crate) fn to_state_machine_snapshot(&self) -> StateMachineSnapshot {
        let snap = self.to_snapshot();
        StateMachineSnapshot::new(snap)
    }

    pub fn curr_seq(&self) -> u64 {
        self.with_sys_data(|s| s.curr_seq())
    }

    pub fn last_membership(&self) -> StoredMembership {
        self.with_sys_data(|s| s.last_membership_ref().clone())
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.with_sys_data(|s| *s.last_applied_mut())
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.with_sys_data(|s| s.nodes_mut().clone())
    }

    // TODO: rename:
    pub(crate) fn with_inner<T>(&self, f: impl FnOnce(&mut LeveledMapData) -> T) -> T {
        let mut inner = self.data.lock().unwrap();
        f(&mut inner)
    }

    /// For testing only.
    /// Replace all immutable levels with the given one.
    #[allow(dead_code)]
    pub(crate) fn replace_immutable_levels(&mut self, b: ImmutableLevels) {
        self.with_inner(|inner| {
            let persisted = inner.immutable.persisted().cloned();
            inner.immutable = Arc::new(ImmutableData::new(b, persisted))
        });
    }

    /// Replace bottom immutable levels and persisted level with compacted data.
    ///
    /// **Important**: Do not drop the compactor within this function when called
    /// under a state machine lock, as dropping may take ~250ms.
    pub fn replace_with_compacted(&self, compactor: &mut Compactor, db: DB) {
        let upto = compactor.compacting_data.latest_level_index();
        let compactor_indexes = compactor.compacting_data.levels().indexes();

        self.with_inner(|inner| {
            let mut levels = inner.immutable.levels().clone();

            info!(
                "replace_with_compacted: compacted upto {:?} immutable levels; my levels: {:?}; compacted levels: {:?}",
                upto,
                levels.indexes(),
                compactor_indexes,
            );

            // If there is immutable levels compacted, remove them.
            if let Some(upto) = upto {
                levels.remove_levels_upto(upto);
            }

            inner.immutable = Arc::new(ImmutableData::new(levels, Some(db)));
        });

        info!("replace_with_compacted: finished replacing the db");
    }

    pub(crate) fn new_compacting_data(&self) -> CompactingData {
        let immutable = self.with_inner(|inner| inner.immutable.clone());
        CompactingData::new(immutable)
    }
}
