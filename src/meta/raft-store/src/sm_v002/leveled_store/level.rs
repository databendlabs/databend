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
use crate::sm_v002::marked::Marked;

/// One level of state machine data.
///
/// State machine data is constructed from multiple levels of modifications, similar to leveldb.
#[derive(Debug, Default)]
pub struct Level {
    data: LevelData,

    /// This level is built with additional modifications `data` on top of the previous level.
    base: Option<Arc<Level>>,
}

impl Level {
    pub(crate) fn new(data: LevelData, base: Option<Arc<Self>>) -> Self {
        Self { data, base }
    }

    pub(crate) fn base(&self) -> Option<&Self> {
        self.base.as_ref().map(|x| x.as_ref())
    }

    pub fn new_level(&mut self) {
        let new = Level {
            data: self.data.new_level(),
            base: None,
        };

        let base = std::mem::replace(self, new);

        self.base = Some(Arc::new(base));
    }

    pub fn data_ref(&self) -> &LevelData {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut LevelData {
        &mut self.data
    }

    pub fn get_base(&self) -> Option<Arc<Self>> {
        self.base.clone()
    }

    pub(crate) fn replace_base(&mut self, b: Option<Arc<Level>>) {
        self.base = b;
    }

    pub fn snapshot(&self) -> Option<Arc<Self>> {
        self.base.clone()
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for Level
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApiRO<K>,
{
    type V = <LevelData as MapApiRO<K>>::V;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        let api = self.data_ref();
        let got = api.get(key).await;

        if got.is_not_found() {
            if let Some(base) = self.base() {
                return base.get(key).await;
            }
        }
        got
    }

    async fn range<'a, T: ?Sized, R>(&'a self, range: R) -> BoxStream<'a, (K, Marked<Self::V>)>
    where
        K: 'a,
        K: Borrow<T> + Clone,
        Self::V: Unpin,
        T: Ord,
        R: RangeBounds<T> + Clone + Send + Sync,
    {
        let a = self.data_ref().range(range.clone()).await;

        let km = KMerge::by(|a: &(K, Marked<Self::V>), b: &(K, Marked<Self::V>)| {
            let (k1, v1) = a;
            let (k2, v2) = b;

            assert_ne!((k1, v1.internal_seq()), (k2, v2.internal_seq()));

            // Put entries with the same key together, smaller internal-seq first
            // Tombstone is always greater.
            (k1, v1.internal_seq()) <= (k2, v2.internal_seq())
        })
        .merge(a);

        let km = if let Some(base) = self.base() {
            let b = base.range(range).await;
            km.merge(b)
        } else {
            km
        };

        // Merge entries with the same key, keep the one with larger internal-seq
        let m = km.coalesce(|(k1, v1), (k2, v2)| {
            if k1 == k2 {
                Ok((k1, Marked::max(v1, v2)))
            } else {
                Err(((k1, v1), (k2, v2)))
            }
        });

        Box::pin(m)
    }
}

#[async_trait::async_trait]
impl<K> MapApi<K> for Level
where
    K: Ord + fmt::Debug + Send + Sync + Unpin + 'static,
    LevelData: MapApi<K>,
{
    async fn set(
        &mut self,
        key: K,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>)
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
        let (_prev, inserted) = self.data_mut().set(key, value).await;
        (prev, inserted)
    }
}
