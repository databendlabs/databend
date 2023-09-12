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

use crate::sm_v002::leveled_store::level_data::LevelData;
use crate::sm_v002::leveled_store::map_api::compacted_get;
use crate::sm_v002::leveled_store::map_api::compacted_range;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::static_leveled_map::StaticLeveledMap;
use crate::sm_v002::marked::Marked;

/// A readonly leveled map store that does not not own the data.
#[derive(Debug)]
pub struct LeveledRef<'d> {
    /// The top level is the newest and writable.
    writable: Option<&'d LevelData>,

    /// The immutable levels.
    frozen: &'d StaticLeveledMap,
}

impl<'d> LeveledRef<'d> {
    pub(in crate::sm_v002) fn new(
        writable: Option<&'d LevelData>,
        frozen: &'d StaticLeveledMap,
    ) -> LeveledRef<'d> {
        Self { writable, frozen }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &'d LevelData> + 'd {
        self.writable.into_iter().chain(self.frozen.iter_levels())
    }
}

#[async_trait::async_trait]
impl<'d, K> MapApiRO<K> for LeveledRef<'d>
where
    K: MapKey + fmt::Debug,
    LevelData: MapApiRO<K>,
{
    async fn get<Q>(&self, key: &Q) -> Marked<K::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_get(key, levels).await
    }

    async fn range<'f, Q, R>(&'f self, range: R) -> BoxStream<'f, (K, Marked<K::V>)>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q> + Clone + Send + Sync,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_range(range, levels).await
    }
}

/// A writable leveled map store that does not not own the data.
#[derive(Debug)]
pub struct LeveledRefMut<'d> {
    /// The top level is the newest and writable.
    writable: &'d mut LevelData,

    /// The immutable levels.
    frozen: &'d StaticLeveledMap,
}

impl<'d> LeveledRefMut<'d> {
    pub(in crate::sm_v002) fn new(
        writable: &'d mut LevelData,
        frozen: &'d StaticLeveledMap,
    ) -> Self {
        Self { writable, frozen }
    }

    #[allow(dead_code)]
    pub(in crate::sm_v002) fn to_leveled_ref<'me>(&'me self) -> LeveledRef<'me> {
        // LeveledRef::new(self.writable, self.frozen)
        LeveledRef::<'me> {
            writable: Some(&*self.writable),
            frozen: self.frozen,
        }
    }

    /// Return an iterator of all levels in new-to-old order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &'_ LevelData> + '_ {
        [&*self.writable]
            .into_iter()
            .chain(self.frozen.iter_levels())
    }
}

// Because `LeveledRefMut` has a mut ref of lifetime 'd,
// `self` must outlive 'd otherwise there will be two mut ref.
#[async_trait::async_trait]
impl<'d, K> MapApiRO<K> for LeveledRefMut<'d>
where
    K: MapKey,
    LevelData: MapApiRO<K>,
{
    async fn get<Q>(&self, key: &Q) -> Marked<K::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_get(key, levels).await
    }

    async fn range<'f, Q, R>(&'f self, range: R) -> BoxStream<'f, (K, Marked<K::V>)>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
    {
        let levels = self.iter_levels();
        compacted_range(range, levels).await
    }
}

#[async_trait::async_trait]
impl<'d, K> MapApi<K> for LeveledRefMut<'d>
where
    K: MapKey,
    LevelData: MapApi<K>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> (Marked<K::V>, Marked<K::V>)
    where
        K: Ord,
    {
        // Get from this level or the base level.
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

    pub(crate) fn leveled_ref_mut(&mut self) -> LeveledRefMut {
        LeveledRefMut::new(&mut self.writable, &self.frozen)
    }

    pub(crate) fn leveled_ref(&self) -> LeveledRef {
        LeveledRef::new(Some(&self.writable), &self.frozen)
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for LeveledMap
where
    K: MapKey + fmt::Debug,
    LevelData: MapApiRO<K>,
{
    async fn get<Q>(&self, key: &Q) -> Marked<K::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        let levels = self.iter_levels();
        compacted_get(key, levels).await
    }

    async fn range<'f, Q, R>(&'f self, range: R) -> BoxStream<'f, (K, Marked<K::V>)>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
    {
        let levels = self.iter_levels();
        compacted_range(range, levels).await
    }
}

#[async_trait::async_trait]
impl<K> MapApi<K> for LeveledMap
where
    K: MapKey,
    LevelData: MapApi<K>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> (Marked<K::V>, Marked<K::V>)
    where
        K: Ord,
    {
        let mut l = self.leveled_ref_mut();
        MapApi::set(&mut l, key, value).await

        // (&mut l).set(key, value).await
    }
}
