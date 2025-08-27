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
use std::fmt;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use compactor::Compactor;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Node;
use futures_util::StreamExt;
use log::debug;
use log::info;
use log::warn;
use map_api::mvcc;
use map_api::util;
use map_api::IOResultStream;
use map_api::MapApiRO;
use map_api::MapKey;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;
use stream_more::KMerge;
use stream_more::StreamMore;
use tokio::sync::Semaphore;

use crate::applier::applier_data::StateMachineView;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::leveled_map::applier_acquirer::WriterPermit;
use crate::leveled_store::leveled_map::compacting_data::CompactingData;
use crate::leveled_store::leveled_map::compactor_acquirer::CompactorAcquirer;
use crate::leveled_store::leveled_map::compactor_acquirer::CompactorPermit;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::value_convert::ValueConvert;
use crate::leveled_store::MapView;
use crate::scoped::Scoped;

#[cfg(test)]
mod acquire_compactor_test;
pub mod applier_acquirer;
pub mod compacting_data;
pub mod compactor;
pub mod compactor_acquirer;
#[cfg(test)]
mod leveled_map_test;
mod map_api_impl;

/// The data of the leveled map.
///
/// The top level is the newest and writable and there is **at most one** candidate writer.
///
/// |                  | writer_semaphore | compactor_semaphore |
/// | :--              | :--              | :--                 |
/// | writable         | RW               |                     |
/// | immutable_levels | R                | RW                  |
/// | persisted        | R                | RW                  |
#[derive(Debug, Default)]
pub struct LeveledMapData {
    /// The top level is the newest and writable.
    ///
    /// Protected by writer_semaphore
    pub(crate) writable: Mutex<Level>,

    /// The immutable levels.
    ///
    /// Protected by compactor_semaphore
    pub(crate) immutable_levels: Mutex<Arc<ImmutableLevels>>,

    /// Protected by compactor_semaphore
    pub(crate) persisted: Mutex<Option<Arc<DB>>>,
}

impl LeveledMapData {
    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        let writable = self.writable.lock().unwrap();
        let mut sys_data = writable.sys_data.lock().unwrap();

        f(&mut sys_data)
    }

    pub(crate) fn writable(&self) -> MutexGuard<'_, Level> {
        let x = self.writable.lock().unwrap();
        x
    }

    pub(crate) fn immutable_levels(&self) -> Arc<ImmutableLevels> {
        let immutable_levels = self.immutable_levels.lock().unwrap();
        immutable_levels.clone()
    }

    pub(crate) fn with_immutable_levels<T>(
        &self,
        f: impl FnOnce(&mut Arc<ImmutableLevels>) -> T,
    ) -> T {
        let mut g = self.immutable_levels.lock().unwrap();
        f(&mut g)
    }

    pub(crate) fn persisted(&self) -> Option<Arc<DB>> {
        let p = self.persisted.lock().unwrap();
        p.clone()
    }

    pub fn with_persisted<T>(&self, f: impl FnOnce(&mut Option<Arc<DB>>) -> T) -> T {
        let mut g = self.persisted.lock().unwrap();
        f(&mut g)
    }

    pub(crate) async fn compacted_view_get<K>(
        &self,
        key: K,
        upto_seq: u64,
    ) -> Result<SeqMarked<K::V>, io::Error>
    where
        K: MapKey,
        K: MapKeyEncode,
        K: MapKeyDecode,
        SeqMarked<K::V>: ValueConvert<SeqMarked>,
        Level: GetTable<K, K::V>,
    {
        // TODO: test it

        {
            let writable = self.writable.lock().unwrap();
            let got = writable.get_table().get(key.clone(), upto_seq).cloned();
            if !got.is_not_found() {
                return Ok(got);
            }
        }

        let immutable_levels = self.immutable_levels();

        for level in immutable_levels.iter_levels() {
            let base = level.get_table().get(key.clone(), upto_seq).cloned();

            if !base.is_not_found() {
                return Ok(base);
            }
        }

        // persisted

        let Some(db) = self.persisted() else {
            return Ok(SeqMarked::new_not_found());
        };

        let map_view = MapView(&db);
        let got = map_view.get(&key).await?;
        Ok(got)
    }

    pub(crate) async fn compacted_view_range<K, R>(
        &self,
        range: R,
        upto_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        K: MapKey,
        R: RangeBounds<K> + Clone + Send + Sync + 'static,
        K: MapKeyEncode,
        K: MapKeyDecode,
        SeqMarked<K::V>: ValueConvert<SeqMarked>,
        Level: GetTable<K, K::V>,
        <K as MapKey>::V: fmt::Debug,
    {
        // TODO: test it

        let mut kmerge = KMerge::by(util::by_key_seq);

        // writable level

        let vec = {
            let writable = self.writable.lock().unwrap();
            let it = writable.get_table().range(range.clone(), upto_seq);
            it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>()
        };

        if vec.len() > 1000 {
            warn!(
                "Level.writable::range(start={:?}, end={:?}) returns big range of len={}",
                range.start_bound(),
                range.end_bound(),
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec)
            // .map(|x| {
            //     debug!("range-item from writable: {:?}", x);
            //     x
            // })
            .map(Ok)
            .boxed();
        kmerge = kmerge.merge(strm);

        // Immutable levels

        let immutable_levels = self.immutable_levels();

        for level in immutable_levels.iter_levels() {
            let index = level.with_sys_data(|s| s.curr_seq());
            let _ = index;
            let it = level.get_table().range(range.clone(), upto_seq);

            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();
            let strm = futures::stream::iter(vec)
                // .map(move |x| {
                //     debug!("range-item from immutable seq={}: {:?}", index, x);
                //     x
                // })
                .map(Ok)
                .boxed();
            kmerge = kmerge.merge(strm);
        }

        // Bottom db level

        if let Some(db) = self.persisted() {
            let map_view = MapView(&db);
            let strm = map_view.range(range.clone()).await?;
            // let strm = strm.map(|x| {
            //     debug!("range-item from db: {:?}", x);
            //     x
            // });
            kmerge = kmerge.merge(strm);
        };

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
    }

    pub fn to_readonly_view(self: &Arc<Self>) -> ReadonlyView {
        ReadonlyView::new(self.clone())
    }
}

/// State machine data organized in multiple levels.
///
/// Similar to leveldb.
///
/// The top level is the newest and writable.
/// Others are immutable.
///
/// - A writer must acquire a permit to write_semaphore.
/// - A compactor must:
///   - acquire the compaction_semaphore first,
///   - then acquire `write_semaphore` to move `writeable` to `immutable_levels`,
#[derive(Debug)]
pub struct LeveledMap {
    /// A semaphore that permits at most one compactor to run.
    pub(crate) compaction_semaphore: Arc<Semaphore>,

    /// Get a permit to write.
    ///
    /// Only one writer is allowed, to achieve serialization.
    /// For historical reason, inserting a tombstone does not increase the seq.
    /// Thus, mvcc isolation with the seq can not completely separate two concurrent writer.
    pub(crate) write_semaphore: Arc<Semaphore>,

    pub data: Arc<LeveledMapData>,
}

impl Default for LeveledMap {
    fn default() -> Self {
        Self {
            // Only one compactor is allowed a time.
            compaction_semaphore: Arc::new(Semaphore::new(1)),
            // Only one writer is allowed a time.
            write_semaphore: Arc::new(Semaphore::new(1)),
            data: Arc::new(LeveledMapData::default()),
        }
    }
}

impl LeveledMap {
    pub fn to_view(&self) -> StateMachineView {
        mvcc::View::new(self.data.clone())
    }

    pub fn to_scoped_view(&self) -> Scoped<StateMachineView> {
        Scoped::new(self.to_view())
    }

    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        let writable = self.data.writable.lock().unwrap();
        let mut sys_data = writable.sys_data.lock().unwrap();

        f(&mut sys_data)
    }

    pub async fn acquire_writer_permit(&self) -> WriterPermit {
        let acquirer = self.new_writer_acquirer();
        let permit = acquirer.acquire().await;
        debug!("WriterPermit acquired");
        permit
    }

    pub fn new_writer_acquirer(&self) -> applier_acquirer::WriterAcquirer {
        applier_acquirer::WriterAcquirer::new(self.write_semaphore.clone())
    }

    /// Return the [`LevelIndex`] of the newest **immutable** data
    pub(crate) fn immutable_level_index(&self) -> Option<LevelIndex> {
        let immutables = self.data.immutable_levels();
        immutables.newest_level_index()
    }

    /// Freeze the current writable level and create a new empty writable level.
    ///
    /// Need writer permit and compactor permit
    pub fn freeze_writable(&self, _writer_permit: &mut WriterPermit) -> Arc<ImmutableLevels> {
        self.do_freeze_writable()
    }

    /// For testing, requires no permit
    pub fn testing_freeze_writable(&self) -> Arc<ImmutableLevels> {
        self.do_freeze_writable()
    }

    pub fn do_freeze_writable(&self) -> Arc<ImmutableLevels> {
        let mut writable = self.data.writable();
        let mut immutables = self.data.immutable_levels.lock().unwrap();

        let new_writable = writable.new_level();
        let new_immutable = std::mem::replace(&mut *writable, new_writable);

        let mut immutable_levels = immutables.as_ref().clone();
        immutable_levels.insert(Immutable::new_from_level(new_immutable));

        *immutables = Arc::new(immutable_levels);

        info!(
            "do_freeze_writable: after writable: {:?}, immutables: {:?}",
            writable,
            immutables.indexes().collect::<Vec<_>>()
        );

        immutables.clone()
    }

    pub fn persisted(&self) -> Option<Arc<DB>> {
        self.data.persisted()
    }

    pub fn with_persisted<T>(&self, f: impl FnOnce(&mut Option<Arc<DB>>) -> T) -> T {
        self.data.with_persisted(f)
    }

    /// Return a reference to the immutable levels.
    pub fn immutable_levels(&self) -> Arc<ImmutableLevels> {
        self.data.immutable_levels()
    }

    pub fn curr_seq(&self) -> u64 {
        self.data.with_sys_data(|s| s.curr_seq())
    }

    pub fn last_membership(&self) -> StoredMembership {
        self.data.with_sys_data(|s| s.last_membership_ref().clone())
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.data.with_sys_data(|s| *s.last_applied_mut())
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.data.with_sys_data(|s| s.nodes_mut().clone())
    }

    /// Replace all immutable levels with the given one.
    #[allow(dead_code)]
    pub(crate) fn replace_immutable_levels(&mut self, b: ImmutableLevels) {
        self.data.with_immutable_levels(|x| *x = Arc::new(b));
    }

    /// Replace bottom immutable levels and persisted level with compacted data.
    ///
    /// **Important**: Do not drop the compactor within this function when called
    /// under a state machine lock, as dropping may take ~250ms.
    pub fn replace_with_compacted(&self, compactor: &mut Compactor, db: DB) {
        info!(
            "replace_with_compacted: compacted upto {:?} immutable levels; my levels: {:?}; compacted levels: {:?}",
            compactor.upto,
            self.data.immutable_levels().indexes().collect::<Vec<_>>(),
            compactor.compacting_data.immutable_levels.indexes().collect::<Vec<_>>(),
        );

        let mut immutables = self.data.immutable_levels().as_ref().clone();

        // If there is immutable levels compacted, remove them.
        if let Some(upto) = compactor.upto {
            immutables.remove_levels_upto(upto);
        }

        // NOTE: Replace data from bottom to top.
        // replace the db first, db contains more data.
        // Otherwise, there is a chance some data is removed from immutables and the new db containing this data is not inserted.

        // replace the persisted
        self.data.with_persisted(|p| {
            *p = Some(Arc::new(db));
        });

        // replace the immutables
        self.data
            .with_immutable_levels(|x| *x = Arc::new(immutables));

        info!("replace_with_compacted: finished replacing the db");
    }

    /// Get a singleton `Compactor` instance specific to `self`.
    ///
    /// This method requires a mutable reference to prevent concurrent access to shared data,
    /// such as `self.immediate_levels` and `self.persisted`, during the construction of the compactor.
    pub(crate) async fn acquire_compactor(&self) -> Compactor {
        let acquirer = self.new_compactor_acquirer();

        let permit = acquirer.acquire().await;

        self.new_compactor(permit)
    }

    pub(crate) fn new_compactor_acquirer(&self) -> CompactorAcquirer {
        CompactorAcquirer::new(self.compaction_semaphore.clone())
    }

    pub fn new_compactor(&self, permit: CompactorPermit) -> Compactor {
        let level_index = self.immutable_level_index();

        Compactor {
            _permit: permit,
            compacting_data: CompactingData::new(self.immutable_levels(), self.persisted()),
            upto: level_index,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadonlyView {
    base_seq: InternalSeq,
    inner: Arc<LeveledMapData>,
}

impl ReadonlyView {
    pub fn new(inner: Arc<LeveledMapData>) -> Self {
        let base_seq = inner.with_sys_data(|s| s.curr_seq());
        Self {
            base_seq: InternalSeq::new(base_seq),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl mvcc::ScopedViewReadonly<UserKey, MetaValue> for ReadonlyView {
    fn base_seq(&self) -> InternalSeq {
        self.base_seq
    }

    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self.inner.compacted_view_get(key, *self.base_seq).await?;
        Ok(got)
    }

    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        let strm = self
            .inner
            .compacted_view_range(range, *self.base_seq)
            .await?;
        Ok(strm)
    }
}
