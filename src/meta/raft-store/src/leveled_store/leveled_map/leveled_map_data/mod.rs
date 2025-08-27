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
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use futures_util::StreamExt;
use log::warn;
use map_api::util;
use map_api::IOResultStream;
use map_api::MapApiRO;
use map_api::MapKey;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::ReadonlyView;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::value_convert::ValueConvert;
use crate::leveled_store::MapView;

mod impl_commit;
mod impl_view_readonly;

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
    pub(crate) inner: Mutex<LeveledMapDataInner>,
}

#[derive(Debug, Default)]
pub struct LeveledMapDataInner {
    /// The top level is the newest and writable.
    ///
    /// Protected by writer_semaphore
    pub(crate) writable: Level,

    /// The immutable levels.
    ///
    /// Protected by compactor_semaphore
    pub(crate) immutable_levels: Arc<ImmutableLevels>,

    /// Protected by compactor_semaphore
    pub(crate) persisted: Option<Arc<DB>>,
}

impl LeveledMapData {
    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        let inner = self.inner.lock().unwrap();
        let mut sys_data = inner.writable.sys_data.lock().unwrap();

        f(&mut sys_data)
    }

    pub(crate) fn inner(&self) -> MutexGuard<'_, LeveledMapDataInner> {
        let x = self.inner.lock().unwrap();
        x
    }

    pub(crate) fn with_inner<T>(&self, f: impl FnOnce(&mut LeveledMapDataInner) -> T) -> T {
        let mut inner = self.inner.lock().unwrap();
        f(&mut inner)
    }

    pub(crate) fn immutable_levels(&self) -> Arc<ImmutableLevels> {
        let inner = self.inner.lock().unwrap();
        inner.immutable_levels.clone()
    }

    pub(crate) fn with_immutable_levels<T>(
        &self,
        f: impl FnOnce(&mut Arc<ImmutableLevels>) -> T,
    ) -> T {
        let mut inner = self.inner.lock().unwrap();
        f(&mut inner.immutable_levels)
    }

    pub(crate) fn persisted(&self) -> Option<Arc<DB>> {
        let inner = self.inner.lock().unwrap();
        inner.persisted.clone()
    }

    pub fn with_persisted<T>(&self, f: impl FnOnce(&mut Option<Arc<DB>>) -> T) -> T {
        let mut inner = self.inner.lock().unwrap();
        f(&mut inner.persisted)
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

        let (immutable_levels, persisted) = {
            let inner = self.inner.lock().unwrap();
            let got = inner
                .writable
                .get_table()
                .get(key.clone(), upto_seq)
                .cloned();
            if !got.is_not_found() {
                return Ok(got);
            }

            (inner.immutable_levels.clone(), inner.persisted.clone())
        };

        for level in immutable_levels.iter_levels() {
            let base = level.get_table().get(key.clone(), upto_seq).cloned();

            if !base.is_not_found() {
                return Ok(base);
            }
        }

        // persisted

        let Some(db) = persisted else {
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
    {
        // TODO: test it

        let mut kmerge = KMerge::by(util::by_key_seq);

        // writable level

        let (vec, immutable_levels, persisted) = {
            let inner = self.inner.lock().unwrap();
            let it = inner.writable.get_table().range(range.clone(), upto_seq);
            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();
            (vec, inner.immutable_levels.clone(), inner.persisted.clone())
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

        for level in immutable_levels.iter_levels() {
            let it = level.get_table().range(range.clone(), upto_seq);

            let vec = it.map(|(k, v)| (k.clone(), v.cloned())).collect::<Vec<_>>();
            let strm = futures::stream::iter(vec).map(Ok).boxed();
            kmerge = kmerge.merge(strm);
        }

        // Bottom db level

        if let Some(db) = persisted {
            let map_view = MapView(&db);
            let strm = map_view.range(range.clone()).await?;
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
