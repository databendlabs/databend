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
use std::future::Future;
use std::ops::RangeBounds;
use std::sync::Arc;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::sm_v002::leveled_store::level_data::LevelData;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::static_leveled_map::StaticLeveledMap;
use crate::sm_v002::leveled_store::util;
use crate::sm_v002::marked::Marked;

/// A readonly leveled map store that does not not own the data.
#[derive(Debug)]
pub struct LeveledRef<'d> {
    /// The top level is the newest and writable.
    writable: &'d LevelData,

    /// The immutable levels, from the oldest to the newest.
    /// levels[0] is the bottom and oldest level.
    frozen: &'d [Arc<LevelData>],
}

impl<'d> LeveledRef<'d> {
    pub(in crate::sm_v002) fn new(
        writable: &'d LevelData,
        frozen: &'d [Arc<LevelData>],
    ) -> LeveledRef<'d> {
        Self { writable, frozen }
    }

    /// Return an iterator of all levels in reverse order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &'d LevelData> + 'd {
        [self.writable]
            .into_iter()
            .chain(self.frozen.iter().map(|x| x.as_ref()).rev())
    }
}

impl<'d, K> MapApiRO<K> for LeveledRef<'d>
where
    K: MapKey + fmt::Debug,
    // &'d LevelData: MapApiRO<K>,
    for<'him> &'him LevelData: MapApiRO<K>,
    // &'him LevelData: MapApiRO<K>,
{
    type GetFut<'f, Q> = impl Future<Output = Marked<K::V>>  + 'f
        where
            Self: 'f,
            K: Borrow<Q>,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn get<'f, Q>(self, key: &'f Q) -> Self::GetFut<'f, Q>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f,
    {
        async move {
            for level_data in self.iter_levels() {
                let got = level_data.get(key).await;
                if !got.is_not_found() {
                    return got;
                }
            }
            Marked::empty()
        }
    }

    type RangeFut<'f, Q, R> = impl Future<Output = BoxStream<'f, (K, Marked<K::V>)>> +  'f
        where
            Self: 'f,
            K: Borrow<Q>,
            R: RangeBounds<Q> + Send + Sync + Clone,
        R:'f,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn range<'f, Q, R>(self, range: R) -> Self::RangeFut<'f, Q, R>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q> + Clone + Send + Sync,
        R: 'f,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f,
    {
        // TODO: &LeveledRef use LeveledRef

        // let levels = self.iter_levels();

        async move {
            let mut km = KMerge::by(util::by_key_seq);

            // for api in levels {
            for api in self.iter_levels() {
                let a = api.range(range.clone()).await;
                km = km.merge(a);
            }

            // Merge entries with the same key, keep the one with larger internal-seq
            let m = km.coalesce(util::choose_greater);

            let x: BoxStream<'_, (K, Marked<K::V>)> = Box::pin(m);
            x
        }
    }
}

/// A writable leveled map store that does not not own the data.
#[derive(Debug)]
pub struct LeveledRefMut<'d> {
    /// The top level is the newest and writable.
    writable: &'d mut LevelData,

    /// The immutable levels, from the oldest to the newest.
    /// levels[0] is the bottom and oldest level.
    frozen: &'d [Arc<LevelData>],
}

impl<'d> LeveledRefMut<'d> {
    pub(in crate::sm_v002) fn new(
        writable: &'d mut LevelData,
        frozen: &'d [Arc<LevelData>],
    ) -> Self {
        Self { writable, frozen }
    }

    pub(in crate::sm_v002) fn to_leveled_ref<'me>(&'me self) -> LeveledRef<'me> {
        // LeveledRef::new(self.writable, self.frozen)
        LeveledRef::<'me> {
            writable: self.writable,
            frozen: self.frozen,
        }
    }

    pub(in crate::sm_v002) fn into_leveled_ref(self) -> LeveledRef<'d> {
        // LeveledRef::new(self.writable, self.frozen)
        LeveledRef::<'d> {
            writable: self.writable,
            frozen: self.frozen,
        }
    }

    /// Return an iterator of all levels in new-to-old order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &'_ LevelData> + '_ {
        [&*self.writable]
            .into_iter()
            .chain(self.frozen.iter().map(|x| x.as_ref()).rev())
    }
}

// Because `LeveledRefMut` has a mut ref of lifetime 'd,
// `self` must outlive 'd otherwise there will be two mut ref.
impl<'d, K> MapApiRO<K> for LeveledRefMut<'d>
where
    K: MapKey,
    for<'him> &'him LevelData: MapApiRO<K>,
{
    type GetFut<'f, Q> = impl Future<Output = Marked<K::V>> + 'f
    where
        Self: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f;

    fn get<'f, Q>(self, key: &'f Q) -> Self::GetFut<'f, Q>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f,
    {
        self.into_leveled_ref().get(key)

        // for level_data in self.iter_levels() {
        //     let got = level_data.get(key).await;
        //     if !got.is_not_found() {
        //         return got;
        //     }
        // }
        // Marked::empty()
    }

    type RangeFut<'f, Q, R> = impl Future<Output = BoxStream<'f, (K, Marked<K::V>)>> +'f
    where
        Self: 'f,
        K: Borrow<Q>,
        R: RangeBounds<Q> + Send + Sync + Clone,
        R: 'f,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f;

    fn range<'f, Q, R>(self, range: R) -> Self::RangeFut<'f, Q, R>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
        R: 'f,
    {
        self.into_leveled_ref().range(range)
    }
}

impl<'d, K> MapApi<K> for LeveledRefMut<'d>
where
    K: MapKey,
    for<'him> &'him LevelData: MapApiRO<K>,
    for<'him> &'him mut LevelData: MapApi<K>,
{
    type RO<'o> = LeveledRef<'o>
    where Self: 'o;

    fn to_ro<'o>(&'o self) -> Self::RO<'o> {
        LeveledRef {
            writable: self.writable,
            frozen: self.frozen,
        }
    }

    type SetFut<'f>  = impl Future<Output = (Marked<K::V>, Marked<K::V>)> + 'f
    where
        Self: 'f,
        'd: 'f;

    fn set<'f>(self, key: K, value: Option<(K::V, Option<KVMeta>)>) -> Self::SetFut<'f>
    where
        K: Ord,
        'd: 'f,
        'd: 'f,
    {
        async move {
            // Get from this level or the base level.
            // let prev = MapApiRO::<'a, String>::get(self, &key).await.clone();
            let x = self.to_leveled_ref();
            let prev = x.get(&key).await.clone();

            // No such entry at all, no need to create a tombstone for delete
            if prev.is_not_found() && value.is_none() {
                return (prev, Marked::new_tomb_stone(0));
            }

            // The data is a single level map and the returned `_prev` is only from that level.
            let (_prev, inserted) = self.writable.set(key, value).await;
            (prev, inserted)
        }
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

    pub(crate) fn leveled_ref_mut<'s>(&'s mut self) -> LeveledRefMut<'s> {
        LeveledRefMut::new(&mut self.writable, self.frozen.levels())
    }

    pub(crate) fn leveled_ref(&self) -> LeveledRef {
        LeveledRef::new(&self.writable, self.frozen.levels())
    }
}

impl<'d, K> MapApiRO<K> for &'d LeveledMap
where
    K: MapKey + fmt::Debug,
    for<'him> &'him LevelData: MapApiRO<K>,
{
    type GetFut<'f, Q> = impl Future<Output = Marked<K::V>> + 'f
        where
            Self: 'f,
            'd: 'f,
            K: Borrow<Q>,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn get<'f, Q>(self, key: &'f Q) -> Self::GetFut<'f, Q>
    where
        'd: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        self.leveled_ref().get(key)
    }

    type RangeFut<'f, Q, R> = impl Future<Output = BoxStream<'f, (K, Marked<K::V>)>>
        where
            Self: 'f,
            'd: 'f,
            K: Borrow<Q>,
            R: RangeBounds<Q> + Send + Sync + Clone,
        R: 'f,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn range<'f, Q, R>(self, range: R) -> Self::RangeFut<'f, Q, R>
    where
        'd: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
        R: 'f,
    {
        self.leveled_ref().range(range)
    }
}

impl<'me, K> MapApi<K> for &'me mut LeveledMap
where
    K: MapKey,
    for<'e> &'e LevelData: MapApiRO<K>,
    for<'him> &'him mut LevelData: MapApi<K>,
{
    type RO<'o> = LeveledRef<'o>
    where Self: 'o;

    fn to_ro<'o>(&'o self) -> Self::RO<'o> {
        LeveledRef::new(&self.writable, self.frozen.levels())
    }

    type SetFut<'f>  = impl Future<Output = (Marked<K::V>, Marked<K::V>)> +'f
    where
        Self: 'f,
        'me: 'f;

    fn set<'f>(self, key: K, value: Option<(K::V, Option<KVMeta>)>) -> Self::SetFut<'f>
    where K: Ord {
        let l = self.leveled_ref_mut();
        MapApi::set(l, key, value)

        // (&mut l).set(key, value).await
    }
}
