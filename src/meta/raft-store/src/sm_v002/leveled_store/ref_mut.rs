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
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;

use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::compacted_get;
use crate::sm_v002::leveled_store::map_api::compacted_range;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::ref_::Ref;
use crate::sm_v002::leveled_store::static_leveled_map::StaticLeveledMap;
use crate::sm_v002::marked::Marked;

/// A writable leveled map that does not not own the data.
#[derive(Debug)]
pub struct RefMut<'d> {
    /// The top level is the newest and writable.
    writable: &'d mut Level,

    /// The immutable levels.
    frozen: &'d StaticLeveledMap,
}

impl<'d> RefMut<'d> {
    pub(in crate::sm_v002) fn new(writable: &'d mut Level, frozen: &'d StaticLeveledMap) -> Self {
        Self { writable, frozen }
    }

    #[allow(dead_code)]
    pub(in crate::sm_v002) fn to_leveled_ref(&self) -> Ref {
        Ref::new(Some(&*self.writable), self.frozen)
    }

    /// Return an iterator of all levels in new-to-old order.
    pub(in crate::sm_v002) fn iter_levels(&self) -> impl Iterator<Item = &'_ Level> + '_ {
        [&*self.writable]
            .into_iter()
            .chain(self.frozen.iter_levels())
    }
}

// Because `LeveledRefMut` has a mut ref of lifetime 'd,
// `self` must outlive 'd otherwise there will be two mut ref.
#[async_trait::async_trait]
impl<'d, K> MapApiRO<K> for RefMut<'d>
where
    K: MapKey,
    Level: MapApiRO<K>,
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
impl<'d, K> MapApi<K> for RefMut<'d>
where
    K: MapKey,
    Level: MapApi<K>,
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
        if prev.not_found() && value.is_none() {
            return (prev, Marked::new_tomb_stone(0));
        }

        // The data is a single level map and the returned `_prev` is only from that level.
        let (_prev, inserted) = self.writable.set(key, value).await;
        (prev, inserted)
    }
}
