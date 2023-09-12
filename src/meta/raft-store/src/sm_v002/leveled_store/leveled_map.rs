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

use std::borrow::Borrow;
use std::fmt;
use std::ops::RangeBounds;
use std::sync::Arc;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::sm_v002::leveled_store::level_data::LevelData;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::static_leveled_map::StaticLeveledMap;
use crate::sm_v002::leveled_store::util;
use crate::sm_v002::marked::Marked;

/// A readonly leveled map store that does not not own the data.
#[derive(Debug)]
pub struct LeveledRef<'a> {
    /// The top level is the newest and writable.
    writable: &'a LevelData,

    /// The immutable levels, from the oldest to the newest.
    /// levels[0] is the bottom and oldest level.
    frozen: &'a [Arc<LevelData>],
}

impl<'a> LeveledRef<'a> {
    pub(in crate::sm_v002) fn new(writable: &'a LevelData, frozen: &'a [Arc<LevelData>]) -> Self {
        Self { writable, frozen }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &LevelData> {
        [self.writable]
            .into_iter()
            .chain(self.frozen.iter().map(|x| x.as_ref()).rev())
    }
}

#[async_trait::async_trait]
impl<'me, 's, K> MapApiRO<'me, 's, K> for LeveledRef<'s>
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApiRO<'me, 's, K>,
{
    type V = <LevelData as MapApiRO<'me, 's, K>>::V;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        for level_data in self.iter_levels() {
            let got = level_data.get(key).await;
            if !got.is_not_found() {
                return got;
            }
        }
        return Marked::empty();
    }

    async fn range<T: ?Sized, R>(&self, range: R) -> BoxStream<'s, (K, Marked<Self::V>)>
    where
        K: Borrow<T> + Clone,
        Self::V: Unpin,
        T: Ord,
        R: RangeBounds<T> + Clone + Send + Sync,
    {
        let mut km = KMerge::by(util::by_key_seq);

        for api in self.iter_levels() {
            let a = api.range(range.clone()).await;
            km = km.merge(a);
        }

        // Merge entries with the same key, keep the one with larger internal-seq
        let m = km.coalesce(util::choose_greater);

        Box::pin(m)
    }
}

/// A writable leveled map store that does not not own the data.
#[derive(Debug)]
pub struct LeveledRefMut<'a> {
    /// The top level is the newest and writable.
    writable: &'a mut LevelData,

    /// The immutable levels, from the oldest to the newest.
    /// levels[0] is the bottom and oldest level.
    frozen: &'a [Arc<LevelData>],
}

impl<'a> LeveledRefMut<'a> {
    pub(in crate::sm_v002) fn new(
        writable: &'a mut LevelData,
        frozen: &'a [Arc<LevelData>],
    ) -> Self {
        Self { writable, frozen }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &LevelData> {
        [&*self.writable]
            .into_iter()
            .chain(self.frozen.iter().map(|x| x.as_ref()).rev())
    }

    pub(in crate::sm_v002) fn to_leveled_ref(&self) -> LeveledRef<'_> {
        LeveledRef::new(self.writable, self.frozen)
    }
}

#[async_trait::async_trait]
impl<'me, 's, K> MapApiRO<'me, 's, K> for LeveledRefMut<'s>
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApiRO<'me, 's, K>,
{
    type V = <LevelData as MapApiRO<'me, 's, K>>::V;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        self.to_leveled_ref().get(key).await
    }

    async fn range<T: ?Sized, R>(&self, range: R) -> BoxStream<'s, (K, Marked<Self::V>)>
    where
        K: Borrow<T> + Clone,
        Self::V: Unpin,
        T: Ord,
        R: RangeBounds<T> + Clone + Send + Sync,
    {
        self.to_leveled_ref().range(range).await
    }
}

#[async_trait::async_trait]
impl<'me, 's, K> MapApi<'me, 's, K> for LeveledRefMut<'s>
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApi<'me, 's, K>,
{
    async fn set<'a>(
        &'a mut self,
        key: K,
        value: Option<(<Self as MapApiRO<'me, 's, K>>::V, Option<KVMeta>)>,
    ) -> (
        Marked<<Self as MapApiRO<'me, 's, K>>::V>,
        Marked<<Self as MapApiRO<'me, 's, K>>::V>,
    )
    where
        K: Ord,
    {
        // Get from this level or the base level.
        // let prev = MapApiRO::<'a, String>::get(self, &key).await.clone();
        let prev = self.get(&key).await.clone();

        // No such entry at all, no need to create a tombstone for delete
        if prev.is_not_found() && value.is_none() {
            return (prev, Marked::new_tomb_stone(0));
        }

        // The data is a single level map and the returned `_prev` is only from that level.
        let (_prev, inserted) = self.writable.set(key, value).await;
        (prev, inserted)
    }
}

/// State machine data organized in multiple levels.
///
/// Similar to leveldb.
///
/// The top level is the newest and writable.
/// Others are immutable.
#[derive(Debug, Default)]
pub struct LeveledMap {
    /// The top level is the newest and writable.
    writable: LevelData,

    /// The immutable levels, from the oldest to the newest.
    /// levels[0] is the bottom and oldest level.
    frozen: StaticLeveledMap,
}

impl LeveledMap {
    pub(crate) fn new(writable: LevelData) -> Self {
        Self {
            writable,
            frozen: Default::default(),
        }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &LevelData> {
        [&self.writable]
            .into_iter()
            .chain(self.frozen.iter_levels())
    }

    /// Freeze the current writable level and create a new writable level.
    pub fn freeze_writable(&mut self) -> &StaticLeveledMap {
        let new_writable = self.writable.new_level();

        let frozen = std::mem::replace(&mut self.writable, new_writable);
        self.frozen.push(Arc::new(frozen));

        &self.frozen
    }

    /// Return an immutable reference to the top level i.e., the writable level.
    pub fn writable_ref(&self) -> &LevelData {
        &self.writable
    }

    /// Return a mutable reference to the top level i.e., the writable level.
    pub fn writable_mut(&mut self) -> &mut LevelData {
        &mut self.writable
    }

    /// Return a reference to the immutable levels.
    pub fn frozen_ref(&self) -> &StaticLeveledMap {
        &self.frozen
    }

    /// Replace all immutable levels with the given one.
    pub(crate) fn replace_frozen_levels(&mut self, b: StaticLeveledMap) {
        self.frozen = b;
    }

    pub(crate) fn leveled_ref_mut<'s>(&mut self) -> LeveledRefMut<'s> {
        LeveledRefMut::new(&mut self.writable, self.frozen.levels())
    }

    pub(crate) fn leveled_ref(&self) -> LeveledRef {
        LeveledRef::new(&self.writable, self.frozen.levels())
    }
}

#[async_trait::async_trait]
impl<'s, K> MapApiRO<'s, 's, K> for LeveledMap
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApiRO<'s, 's, K>,
{
    type V = <LevelData as MapApiRO<'s, 's, K>>::V;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        self.leveled_ref().get(key).await
    }

    async fn range<T: ?Sized, R>(&self, range: R) -> BoxStream<'s, (K, Marked<Self::V>)>
    where
        K: Borrow<T> + Clone,
        Self::V: Unpin,
        T: Ord,
        R: RangeBounds<T> + Clone + Send + Sync,
    {
        self.leveled_ref().range(range).await
    }
}

#[async_trait::async_trait]
impl<'s, K> MapApi<'s, 's, K> for LeveledMap
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApi<'s, 's, K>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(<Self as MapApiRO<'s, 's, K>>::V, Option<KVMeta>)>,
    ) -> (
        Marked<<Self as MapApiRO<'s, 's, K>>::V>,
        Marked<<Self as MapApiRO<'s, 's, K>>::V>,
    )
    where
        K: Ord,
    {
        self.leveled_ref_mut::<'s>().set(key, value).await
    }
}
