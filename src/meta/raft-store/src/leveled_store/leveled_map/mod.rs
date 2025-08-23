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
use std::io::Error;
use std::range::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;

use compactor::Compactor;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use futures_util::StreamExt;
use log::info;
use log::warn;
use map_api::util;
use map_api::IOResultStream;
use map_api::MapApiRO;
use map_api::MapKey;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;
use tokio::sync::Semaphore;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;
use crate::leveled_store::leveled_map::compactor_acquirer::CompactorAcquirer;
use crate::leveled_store::leveled_map::compactor_acquirer::CompactorPermit;
use crate::leveled_store::MapView;

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
#[derive(Debug, Default)]
pub struct LeveledMapData {
    /// The top level is the newest and writable.
    pub(crate) writable: Mutex<Level>,

    /// The immutable levels.
    pub(crate) immutable_levels: ImmutableLevels,

    pub(crate) persisted: Option<DB>,
}

impl LeveledMapData {
    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        let mut writable = self.writable.lock().unwrap();
        let mut sys_data = writable.sys_data.lock().unwrap();

        f(&mut sys_data)
    }

    pub(crate) async fn compacted_view_get<K>(
        &self,
        key: K,
        upto_seq: u64,
    ) -> Result<SeqMarked<K::V>, io::Error>
    where
        K: MapKey,
    {
        // TODO: test it

        {
            let writable = self.writable.lock().unwrap();
            let got = writable.get_table().get(key.clone(), upto_seq).cloned();
            if !got.is_not_found() {
                return Ok(got);
            }
        }

        for level in self.immutable_levels.iter_levels() {
            let base = level.get_table().get(key.clone(), upto_seq).cloned();

            if !base.is_not_found() {
                return Ok(base);
            }
        }

        let Some(db) = self.persisted.as_ref() else {
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

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        kmerge = kmerge.merge(strm);

        // Immutable levels

        for level in self.immutable_levels.iter_levels() {
            let it = level.get_table().range(range.clone(), upto_seq);

            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();
            let strm = futures::stream::iter(vec).map(Ok).boxed();
            kmerge = kmerge.merge(strm);
        }

        // Bottom db level

        if let Some(db) = self.persisted.as_ref() {
            let map_view = MapView(&db);
            let strm = map_view.range(range.clone()).await?;
            kmerge = kmerge.merge(strm);
        };

        // Merge entries with the same key, keep the one with larger internal-seq
        let coalesce = kmerge.coalesce(util::merge_kv_results);

        Ok(coalesce.boxed())
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
    compaction_semaphore: Arc<Semaphore>,

    /// Get a permit to write.
    ///
    /// Only one writer is allowed, to achieve serialization.
    /// For historical reason, inserting a tombstone does not increase the seq.
    /// Thus, mvcc isolation with the seq can not completely separate two concurrent writer.
    write_semaphore: Arc<Semaphore>,

    data: Arc<LeveledMapData>,
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
    pub fn sys_data(&self) -> Arc<SysData> {
        self.data.writable.lock().unwrap().sys_data.clone()
    }

    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        let mut writable = self.data.writable.lock().unwrap();
        let mut sys_data = writable.sys_data.lock().unwrap();

        f(&mut sys_data)
    }

    // TODO:
    pub(crate) fn clear(&mut self) {
        self.writable = Default::default();
        self.immutable_levels = Default::default();
        self.persisted = None;
    }

    /// Return the [`LevelIndex`] of the newest **immutable** data
    pub(crate) fn immutable_level_index(&self) -> Option<LevelIndex> {
        let newest = self.data.immutable_levels.newest()?;
        Some(*newest.level_index())
    }

    /// Return an iterator of all levels in reverse order.
    /// TODO:
    pub(crate) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        [&self.writable]
            .into_iter()
            .chain(self.data.immutable_levels.iter_levels())
    }

    /// Return the top level and an iterator of all immutable levels, in newest to oldest order.
    pub(crate) fn iter_shared_levels(&self) -> (Option<&Level>, impl Iterator<Item = &Immutable>) {
        (
            Some(&self.writable),
            self.immutable_levels.iter_immutable_levels(),
        )
    }

    /// Freeze the current writable level and create a new empty writable level.
    pub fn freeze_writable(&mut self) -> &ImmutableLevels {
        let new_writable = self.writable.new_level();

        let immutable = std::mem::replace(&mut self.writable, new_writable);
        self.immutable_levels
            .push(Immutable::new_from_level(immutable));

        &self.immutable_levels
    }

    /// Return an immutable reference to the top level i.e., the writable level.
    pub fn writable_ref(&self) -> &Level {
        &self.writable
    }

    /// Return a mutable reference to the top level i.e., the writable level.
    pub fn writable_mut(&mut self) -> &mut Level {
        &mut self.writable
    }

    pub fn sys_data_mut(&mut self) -> &mut SysData {
        self.writable.sys_data_mut()
    }

    pub fn persisted(&self) -> Option<&DB> {
        self.data.persisted.as_ref()
    }

    pub fn persisted_mut(&mut self) -> &mut Option<DB> {
        &mut self.persisted
    }

    /// Return a reference to the immutable levels.
    pub fn immutable_levels_ref(&self) -> &ImmutableLevels {
        &self.immutable_levels
    }

    /// Replace all immutable levels with the given one.
    #[allow(dead_code)]
    pub(crate) fn replace_immutable_levels(&mut self, b: ImmutableLevels) {
        self.immutable_levels = b;
    }

    /// Replace bottom immutable levels and persisted level with compacted data.
    ///
    /// **Important**: Do not drop the compactor within this function when called
    /// under a state machine lock, as dropping may take ~250ms.
    pub fn replace_with_compacted(&mut self, compactor: &mut Compactor, db: DB) {
        let len = compactor.immutable_levels.len();
        let corresponding_index = compactor
            .immutable_levels
            .levels()
            .get(len - 1)
            .map(|l| l.level_index())
            .copied();

        assert_eq!(
            compactor.since, corresponding_index,
            "unexpected change to sm during compaction"
        );

        let levels = self.immutable_levels.levels();

        let newly_added = levels.split_off(len);
        *levels = newly_added;

        self.persisted = Some(db);

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

    pub(crate) fn new_compactor(&self, permit: CompactorPermit) -> Compactor {
        Compactor {
            _permit: permit,
            immutable_levels: self.immutable_levels.clone(),
            db: self.persisted.clone(),
            since: self.immutable_level_index(),
        }
    }
}
